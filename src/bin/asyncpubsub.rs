use bytes::Bytes;
use rumqttc::v5::{AsyncClient, Event, Incoming, MqttOptions, mqttbytes::QoS};
use std::{sync::Arc, time::Duration};
use tokio::{
  io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, stdin, stdout},
  join,
  sync::{Mutex, mpsc},
  task,
  time::sleep,
};

macro_rules! async_write {
  ($dst:expr, $($arg:tt)*) => {
    {
      let mut dst = $dst.lock().await;
      dst.write_all(format!($($arg)*).as_bytes()).await.unwrap();
      dst.flush().await.unwrap();
    }
  };
}

macro_rules! async_writeln {
  ($dst:expr $(,)?) => {
    async_write!($dst, "\n");
  };
  ($dst:expr, $($arg:tt)*) => {
    {
      let mut dst = $dst.lock().await;
      dst.write_all({
        let mut tmp = format!($($arg)*);
        tmp.push('\n');
        tmp
      }.as_bytes()).await.unwrap();
      dst.flush().await.unwrap();
    }
  };
}

#[tokio::main]
async fn main() {
  let out = BufWriter::new(stdout());
  let out = Arc::new(Mutex::new(out));

  async_write!(out, "Prompt message: ");

  let mut inp = BufReader::new(stdin());
  let mut buf = String::new();
  let _count = inp.read_line(&mut buf).await.unwrap();
  let mut iter = buf.split_ascii_whitespace();
  let message: String = iter.next().unwrap().parse().unwrap();

  let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
  mqttoptions.set_keep_alive(Duration::from_secs(5));
  let (client, mut eventloop) = AsyncClient::new(mqttoptions, 1000);
  client
    .subscribe("hello/mqtt", QoS::AtMostOnce)
    .await
    .unwrap();

  let (tx, mut rx) = mpsc::unbounded_channel();

  let subcribe_out = out.clone();
  let subcribe_task = task::spawn(task::coop::cooperative(async move {
    async_writeln!(
      subcribe_out,
      "Subcriber running on thread: {:?}",
      std::thread::current().id()
    );
    while let Some(data) = rx.recv().await {
      async_writeln!(subcribe_out, "Receive: {:?}", data);
      sleep(Duration::from_millis(5)).await;
    }
  }));

  let publish_out = out.clone();
  let publish_task = task::spawn(task::coop::cooperative(async move {
    async_writeln!(
      publish_out,
      "Publisher running on thread: {:?}",
      std::thread::current().id()
    );

    for i in 0..10 {
      let data = Bytes::from(format!("{} {}", message, i));
      client
        .publish("hello/mqtt", QoS::AtMostOnce, false, data.clone())
        .await
        .unwrap();
      async_writeln!(publish_out, "Publish: {:?}", data);
      sleep(Duration::from_millis(5)).await;
    }
  }));

  let eventloop_out = out.clone();
  let eventloop_fut = async move {
    async_writeln!(
      eventloop_out,
      "EL Running on thread: {:?}",
      std::thread::current().id()
    );
    loop {
      match eventloop.poll().await {
        Ok(notification) => {
          if let Event::Incoming(Incoming::Publish(publish)) = notification {
            // publish.properties.p
            tx.send(publish.payload).unwrap();
          }
        }
        Err(e) => {
          async_writeln!(eventloop_out, "{}", e)
        }
      };
    }
  };
  let eventloop_task = task::spawn(task::coop::unconstrained(eventloop_fut));
  let _ = join!(eventloop_task, subcribe_task, publish_task);
}
