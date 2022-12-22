use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>
    },
}

type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(32);
    let tx2 = tx.clone();

    let t1 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();

        tx.send(Command::Get {
            key: String::from("foo"),
            resp: resp_tx
        }).await.unwrap();

        let res = resp_rx.await;
        println!("Got = {:?}", res.unwrap().unwrap().unwrap());
    });
    
    let t2 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();

        tx2.send(Command::Set {
            key: String::from("foo"),
            val: "bar".into(),
            resp: resp_tx
        }).await.unwrap();

        let res = resp_rx.await;
        println!("Got = {:?}", res.unwrap().unwrap());
    });

    use mini_redis::client;
    let manager = tokio::spawn(async move {
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        while let Some(cmd) = rx.recv().await {
            use Command::*;

            match cmd {
                Get {key, resp} => {
                    let res = client.get(&key).await;

                    let _ = resp.send(res);
                }
                Set {key, val, resp} => {
                    let res = client.set(&key, val).await;

                    let _ = resp.send(res);
                }
            }
        }
    });

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();
}