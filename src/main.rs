use bytes::Bytes;
use rumqttc::v5::{
  AsyncClient, Event, Incoming, MqttOptions,
  mqttbytes::{QoS, v5::SubscribeProperties},
};
use std::time::Duration;
use tokio::{join, sync::mpsc, task};

#[tokio::main]
async fn main() {
  let mut mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
  mqttoptions.set_keep_alive(Duration::from_secs(5));
  let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
  // client
  //   .subscribe_with_properties(
  //     "hello/mqtt",
  //     QoS::AtMostOnce,
  //     SubscribeProperties {
  //       id: Some(50),
  //       user_properties: vec![],
  //     },
  //   )
  //   .await
  //   .unwrap();
  // client
  //   .subscribe_with_properties(
  //     "hello/+",
  //     QoS::AtMostOnce,
  //     SubscribeProperties {
  //       id: Some(40),
  //       user_properties: vec![],
  //     },
  //   )
  //   .await
  //   .unwrap();
  // client
  //   .subscribe_with_properties(
  //     "hello/#",
  //     QoS::AtMostOnce,
  //     SubscribeProperties {
  //       id: Some(30),
  //       user_properties: vec![],
  //     },
  //   )
  //   .await
  //   .unwrap();

  client
    .subscribe("hello/mqtt", QoS::AtMostOnce)
    .await
    .unwrap();

  client.subscribe("hello/+", QoS::AtMostOnce).await.unwrap();
  client.subscribe("hello/#", QoS::AtMostOnce).await.unwrap();

  let (tx, mut rx) = mpsc::unbounded_channel();

  let subcribe_task = task::spawn(async move {
    while let Some(data) = rx.recv().await {
      // println!("Receive: {:?}", data);
    }
  });

  let publish_task = task::spawn(async move {
    for i in 0..1 {
      let data = Bytes::from(vec![i; i as usize]);
      client
        .publish("hello/mqtt", QoS::AtLeastOnce, false, data.clone())
        .await
        .unwrap();
      // println!("Publish: {:?}", data);
      // time::sleep(Duration::from_millis(100)).await;
    }
  });

  let eventloop_fut = async {
    loop {
      match eventloop.poll().await {
        Ok(notification) => {
          println!("Received = {:#?}", notification);
          if let Event::Incoming(Incoming::Publish(publish)) = notification {
            tx.send(publish.payload).unwrap();
          }
          // time::sleep(Duration::from_millis(1000)).await;
        }
        Err(e) => {
          println!("{}", e)
        }
      };
    }
  };
  let eventloop_task = task::coop::unconstrained(eventloop_fut);
  join!(eventloop_task, subcribe_task, publish_task);
}
