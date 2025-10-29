use bytes::Bytes;
use rumqttc::v5::{AsyncClient, Event, Incoming, MqttOptions, mqttbytes::QoS};
use std::{sync::Arc, time::Duration};
use tokio::{
  io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, stdin, stdout},
  join,
  sync::{Mutex, mpsc},
  task::{self, JoinSet},
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

async fn simulate_device<W: AsyncWrite + Unpin + Send + 'static>(
  id: usize,
  out: Arc<Mutex<W>>,
  message: String,
) {
  const HOST: &'static str = "localhost";
  const PORT: u16 = 1883;
  const KEEP_ALIVE_DURATION: u64 = 5;

  let mut mqtt_options = MqttOptions::new(id.to_string(), HOST, PORT);
  mqtt_options.set_keep_alive(Duration::from_secs(KEEP_ALIVE_DURATION));

  let (client, mut event_loop) = AsyncClient::new(mqtt_options, 100);

  client
    .subscribe("aps/100devices", QoS::AtMostOnce)
    .await
    .unwrap();

  let (tx, mut rx) = mpsc::unbounded_channel();

  let subcribe_out = out.clone();
  let subcribe_task = task::spawn(task::coop::cooperative(async move {
    async_writeln!(
      subcribe_out,
      "Device-{} Subcriber running on thread: {:?}",
      id,
      std::thread::current().id()
    );
    while let Some(data) = rx.recv().await {
      async_writeln!(subcribe_out, "Device-{} Receive: {:?}", id, data);
      sleep(Duration::from_millis(5)).await;
    }
  }));

  let publish_out = out.clone();
  let publish_task = task::spawn(task::coop::cooperative(async move {
    async_writeln!(
      publish_out,
      "Device-{} Publisher running on thread: {:?}",
      id,
      std::thread::current().id()
    );

    for i in 0..1 {
      let data = Bytes::from(format!("{} {}", message, i));
      client
        .publish("hello/mqtt", QoS::AtMostOnce, false, data.clone())
        .await
        .unwrap();
      async_writeln!(publish_out, "Device-{} Publish: {:?}", id, data);
      sleep(Duration::from_millis(5)).await;
    }
  }));

  let event_loop_out = out.clone();
  let event_loop_fut = async move {
    async_writeln!(
      event_loop_out,
      "Device-{} EL Running on thread: {:?}",
      id,
      std::thread::current().id()
    );
    loop {
      match event_loop.poll().await {
        Ok(notification) => {
          if let Event::Incoming(Incoming::Publish(publish)) = notification {
            tx.send(publish.payload).unwrap();
          }
        }
        Err(e) => {
          async_writeln!(event_loop_out, "Device-{}: {}", id, e)
        }
      };
    }
  };
  let event_loop_task = task::spawn(task::coop::unconstrained(event_loop_fut));
  let _ = join!(event_loop_task, subcribe_task, publish_task);
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

  let mut set = JoinSet::new();

  for i in 0..100 {
    set.spawn(simulate_device(i, out.clone(), message.clone()));
  }

  let _ = set.join_all().await;
}
