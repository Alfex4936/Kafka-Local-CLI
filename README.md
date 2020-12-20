<div align="center">
<p>
    <img src="https://github.com/Alfex4936/Kafka-Local-CLI/blob/main/imgs/MAIN.png">
    <img src="https://github.com/Alfex4936/Kafka-Local-CLI/blob/main/imgs/SERVER_RUN.png">
    <img src="https://github.com/Alfex4936/Kafka-Local-CLI/blob/main/imgs/SERVER_OFF.png">
    <img src="https://github.com/Alfex4936/Kafka-Local-CLI/blob/main/imgs/TOPIC_CREATE.png">
    <img src="https://github.com/Alfex4936/Kafka-Local-CLI/blob/main/imgs/TOPIC_DESC.png">
    <img src="https://github.com/Alfex4936/Kafka-Local-CLI/blob/main/imgs/TOPIC_LIST.png">
</p>
<h1>Kafka local CLI</h1>

<a href="https://hits.seeyoufarm.com"><img src="https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fgithub.com%2FAlfex4936%2FKafka-Local-CLI&count_bg=%23F12525&title_bg=%23555555&icon=apachekafka.svg&icon_color=%23E7E7E7&title=%3A&edge_flat=false"/></a>

[Apache Kafka](https://kafka.apache.org/)

</div>

## Install

```console
pip install PyInquirer
```

Download [cli.py](https://github.com/Alfex4936/Kafka-Local-CLI/blob/main/cli.py) into your kafka folder.

## Usage

Run [cli.py](https://github.com/Alfex4936/Kafka-Local-CLI/blob/main/cli.py) in your kafka folder

By default, zookeeper address is "localhost:2181" and
kafka address is "localhost:9092"

```console
~/kafka$ python cli.py

(It'll run zookeeper at localhost:2181 and kafka at localhost:9092)

~/kafka$ python cli.py -zk localhost:2000 -kf localhost:4000
```