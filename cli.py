from __future__ import print_function, unicode_literals

import os
import shlex
import socket
import subprocess
import time

from PyInquirer import Separator, Token, prompt, style_from_dict


ZK_STATUS = False  # Is zookeeper running?
KF_STATUS = False  # Is kafka running?

style = style_from_dict(
    {
        Token.Separator: "#cc5454",
        Token.QuestionMark: "#673ab7 bold",
        Token.Selected: "#cc5454",  # default
        Token.Pointer: "#673ab7 bold",
        Token.Instruction: "",  # default
        Token.Answer: "#f44336 bold",
        Token.Question: "",
    }
)


def runServer(server):
    global ZK_STATUS, KF_STATUS
    ERROR = 0
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    if server == "zookeeper":
        result = subprocess.Popen(
            shlex.split(
                "nohup zookeeper-server-start.sh ./config/zookeeper.properties"
            ),  # run in other process
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setpgrp,
        )

        for line in iter(result.stdout.readline, b""):  # sentinel
            line = line.decode("utf-8")
            if "WARN" in line:
                print("ZK LOG >>>", line)
            if "BIND" in line:
                print("ZK LOG >>>", line)
            if "ERROR" in line:
                ERROR = 1
                print("ZK LOG >>>", line)
            if not ERROR and sock.connect_ex(("localhost", 2181)) == 0:
                ERROR = 0
                ZK_STATUS = True
                sock.close()
                break

        result.stdout.close()
        if ERROR:
            ZK_STATUS = False
            sock.close()
            return -1
        return 1

    elif server == "kafka":
        result2 = subprocess.Popen(
            shlex.split(
                "nohup kafka-server-start.sh ./config/server.properties"
            ),  # run in other process
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setpgrp,
        )
        time.sleep(10)  # give it a time to check connection.

        for line in iter(result2.stdout.readline, b""):  # sentinel
            line = line.decode("utf-8")
            print(line)
            if "WARN" in line:
                print("KAFKA LOG >>>", line)
            if "BIND" in line:
                print("KAFKA LOG >>>", line)
            if "ERROR" in line:
                ERROR = 1
                print("KAFKA LOG >>>", line)
            if not ERROR and sock.connect_ex(("localhost", 9092)) == 0:
                ERROR = 0
                KF_STATUS = True
                sock.close()
                break

        result2.stdout.close()
        if ERROR:
            KF_STATUS = False
            sock.close()
            return -1
        return 1


def turnOff(server):
    global ZK_STATUS, KF_STATUS
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if server == "kafka":
        result = subprocess.Popen(shlex.split("kafka-server-stop.sh"))
        while sock.connect_ex(("localhost", 9092)) == 0:
            time.sleep(1)

        KF_STATUS = False
        result.terminate()
        sock.close()
    elif server == "zookeeper":
        result = subprocess.Popen(shlex.split("zookeeper-server-stop.sh"))
        while sock.connect_ex(("localhost", 2181)) == 0:
            time.sleep(1)

        ZK_STATUS = False
        result.terminate()
        sock.close()


def createTopic():
    questions = [
        {
            "type": "input",
            "message": "A name of topic to create: (STR)",
            "name": "topic_name",
            "default": "sample-topic",
        },
        {
            "type": "input",
            "message": "How many partitions: (INT)",
            "name": "partitions",
            "default": "3",
        },
        {
            "type": "input",
            "message": "A replication factor: (INT)",
            "name": "replication",
            "default": "1",
        },
    ]

    answers = prompt(questions, style=style)
    result = subprocess.Popen(
        shlex.split(
            f"kafka-topics.sh --zookeeper localhost:2181 --topic {answers['topic_name']} --create --partitions {answers['partitions']} --replication-factor {answers['replication']}"
        ),  # run in other process
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    for line in iter(result.stdout.readline, b""):
        line = line.decode("utf-8")
        print(line)

    result.stdout.close()


def deleteTopic():
    questions = [
        {
            "type": "input",
            "message": "A name of topic to remove: (STR)",
            "name": "topic_name",
            "default": "sample-topic",
        },
    ]

    answers = prompt(questions, style=style)
    result = subprocess.Popen(
        shlex.split(
            f"kafka-topics.sh --zookeeper localhost:2181 --delete --topic {answers['topic_name']}"
        ),  # run in other process
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    for line in iter(result.stdout.readline, b""):
        line = line.decode("utf-8")
        print(line)

    result.stdout.close()


if __name__ == "__main__":
    try:
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if sock.connect_ex(("localhost", 2181)) == 0:  # Check everytime
                ZK_STATUS = True
                print(">>> Zookeeper is connected.")
            else:
                ZK_STATUS = False
                print(">>> Zookeeper is not connected.")
            sock.close()

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if sock.connect_ex(("localhost", 9092)) == 0:  # Check everytime
                KF_STATUS = True
                print(">>> Kafka is connected.")
            else:
                KF_STATUS = False
                print(">>> Kafka is not connected.")

            sock.close()

            if ZK_STATUS and KF_STATUS:
                break

            while not (ZK_STATUS and KF_STATUS):  # until servers are available
                questions = [
                    {
                        "type": "list",
                        "message": "Select a server to start",
                        "name": "server",
                        "choices": ["Run Zookeeper", "Run Kafka", "Skip"],
                    },
                ]

                answers = prompt(questions, style=style)
                # pprint(answers)
                if "server" in answers:
                    if answers["server"] == "Run Zookeeper":
                        runServer("zookeeper")
                    elif answers["server"] == "Run Kafka":
                        runServer("kafka")
                    elif answers["server"] == "Skip":
                        break

            if answers["server"] == "Skip":
                break

        while True:
            questions = [
                {
                    "type": "list",
                    "message": "Topic options",
                    "name": "topic",
                    "choices": [
                        Separator("= Topic tools ="),
                        {"name": "Create"},
                        {"name": "Delete"},
                        {"name": "Skip to next menu"},
                    ],
                },
                {
                    "type": "list",
                    "message": "Server options",
                    "name": "server",
                    "choices": [
                        Separator("= Server options (!Kafka first!) ="),
                        {"name": "Turn off Kafka"},
                        {"name": "Turn off Zookeeper"},
                        {"name": "Skip"},
                    ],
                    "when": lambda answers: answers["topic"] == "Skip to next menu",
                },
            ]

            answers = prompt(questions, style=style)
            if "server" in answers:
                if answers["server"] == "Turn off Kafka":
                    turnOff("kafka")
                elif answers["server"] == "Turn off Zookeeper":
                    turnOff("zookeeper")

            if "topic" in answers:
                if answers["topic"] == "Create":
                    createTopic()
                elif answers["topic"] == "Delete":
                    deleteTopic()

            questions = [
                {
                    "type": "confirm",
                    "message": "= Do you want to exit? =",
                    "name": "exit",
                    "default": False,
                },
            ]
            answers = prompt(questions, style=style)
            if answers["exit"]:
                break
    except Exception as e:  # General exceptions
        print(e)
    except KeyboardInterrupt:
        print("Pressed CTRL+C...")
    finally:
        if ZK_STATUS:
            print(
                "It seems zookeeper server is running, you can stop it from your terminal."
            )
        if KF_STATUS:
            print(
                "It seems kafka server is running, you can stop it from your terminal."
            )
        print("\nExiting...")
