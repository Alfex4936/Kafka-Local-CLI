from __future__ import print_function, unicode_literals

import argparse
import os
import shlex
import socket
import subprocess
import time

from configparser import ConfigParser
from PyInquirer import Separator, Token, prompt, style_from_dict


ZK_STATUS = False  # Is zookeeper running?
KF_STATUS = False  # Is kafka running?

ZK_IP = "localhost"
ZK_PORT = "2181"
KF_IP = "localhost"
KF_PORT = "9092"

config = ConfigParser()

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
    config.read("pid.ini")

    ERROR = 0
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    if server == "zookeeper":
        zKProcess = subprocess.Popen(
            shlex.split(
                "nohup zookeeper-server-start.sh ./config/zookeeper.properties"
            ),  # run in other process
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setpgrp,
        )
        print("ZK PID >>>", zKProcess.pid)
        config.set("PID", "ZOOKEEPER_SERVER", str(zKProcess.pid))

        for line in iter(zKProcess.stdout.readline, b""):  # sentinel
            line = line.decode("utf-8")
            if "WARN" in line:
                print("ZK LOG >>>", line)
            if "BIND" in line:
                print("ZK LOG >>>", line)
            if "ERROR" in line:
                ERROR = 1
                print("ZK LOG >>>", line)
            if not ERROR and sock.connect_ex((ZK_IP, int(ZK_PORT))) == 0:
                ERROR = 0
                ZK_STATUS = True
                sock.close()
                break

        zKProcess.stdout.close()

        if ERROR:
            ZK_STATUS = False
            sock.close()
            return -1

        with open("pid.ini", "w") as f:
            config.write(f)

        return 1

    elif server == "kafka":
        kfProcess = subprocess.Popen(
            shlex.split(
                "nohup kafka-server-start.sh ./config/server.properties"
            ),  # run in other process
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setpgrp,
        )
        print("KAFKA PID >>>", kfProcess.pid)
        config.set("PID", "KAFKA_SERVER", str(kfProcess.pid))
        time.sleep(10)  # give it a time to check connection.

        for line in iter(kfProcess.stdout.readline, b""):  # sentinel
            line = line.decode("utf-8")
            if "WARN" in line:
                print("KAFKA LOG >>>", line)
            if "BIND" in line:
                print("KAFKA LOG >>>", line)
            if "ERROR" in line:
                ERROR = 1
                print("KAFKA LOG >>>", line)
            if not ERROR and sock.connect_ex((KF_IP, int(KF_PORT))) == 0:
                ERROR = 0
                KF_STATUS = True
                sock.close()
                break

        kfProcess.stdout.close()
        if ERROR:
            KF_STATUS = False
            sock.close()
            return -1

        with open("pid.ini", "w") as f:
            config.write(f)
        return 1
    else:
        sock.close()
        return -1


def turnOff(server):
    global ZK_STATUS, KF_STATUS

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if server == "kafka":
        result = subprocess.Popen(shlex.split("kafka-server-stop.sh"))

        while (
            sock.connect_ex((KF_IP, int(KF_PORT))) == 0
        ):  # wait for the connection is closed
            time.sleep(1)

        KF_STATUS = False
        result.terminate()
        sock.close()
    elif server == "zookeeper":
        result = subprocess.Popen(shlex.split("zookeeper-server-stop.sh"))

        while (
            sock.connect_ex((ZK_IP, int(ZK_PORT))) == 0
        ):  # wait for the connection is closed
            time.sleep(1)

        ZK_STATUS = False
        result.terminate()
        sock.close()


def changeIP(server):
    global ZK_IP, ZK_PORT
    global KF_IP, KF_PORT

    if server == "zookeeper":
        questions = [
            {
                "type": "input",
                "message": f"Zookeeper Server Address: ({ZK_IP}:{ZK_PORT})",
                "name": "zk",
                "default": "localhost:2181",
            },
            {
                "type": "confirm",
                "message": "= Do you want to continue? =",
                "name": "exit",
                "default": False,
            },
        ]

        answers = prompt(questions, style=style)
        if answers["exit"]:
            print(">>> Terminating zookeeper...")
            turnOff(server)
            ZK_IP, ZK_PORT = answers["zk"].split(":")
            runServer("zookeeper")
        else:
            return -1

    elif server == "kafka":
        questions = [
            {
                "type": "input",
                "message": f"Kafka Server Address: ({KF_IP}:{KF_PORT})",
                "name": "kf",
                "default": "localhost:9092",
            },
            {
                "type": "confirm",
                "message": "= Do you want to continue? =",
                "name": "exit",
                "default": False,
            },
        ]

        answers = prompt(questions, style=style)
        if answers["exit"]:
            print(">>> Terminating kafka...")
            turnOff(server)
            KF_IP, KF_PORT = answers["kf"].split(":")
            runServer("kafka")
        else:
            return -1


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
            f"kafka-topics.sh --zookeeper {ZK_IP}:{ZK_PORT} --topic {answers['topic_name']} --create --partitions {answers['partitions']} --replication-factor {answers['replication']}"
        ),
        stdout=subprocess.PIPE,
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
            f"kafka-topics.sh --zookeeper {ZK_IP}:{ZK_PORT} --delete --topic {answers['topic_name']}"
        ),
        stdout=subprocess.PIPE,
    )

    for line in iter(result.stdout.readline, b""):
        line = line.decode("utf-8")
        print(line)

    result.stdout.close()


def getTopicList():
    result = subprocess.Popen(
        shlex.split(f"kafka-topics.sh --zookeeper {ZK_IP}:{ZK_PORT} --list"),
        stdout=subprocess.PIPE,
    )

    index = 0
    for line in iter(result.stdout.readline, b""):
        index += 1
        line = line.decode("utf-8")
        print(f"{index}: {line}")

    result.stdout.close()


def getDescription():
    questions = [
        {
            "type": "input",
            "message": "A name of topic to see the description: (STR)",
            "name": "topic_name",
            "default": "sample-topic",
        },
    ]

    answers = prompt(questions, style=style)
    result = subprocess.Popen(
        shlex.split(
            f"kafka-topics.sh --zookeeper {ZK_IP}:{ZK_PORT} --topic {answers['topic_name']} --describe"
        ),
        stdout=subprocess.PIPE,
    )
    for line in iter(result.stdout.readline, b""):
        line = line.decode("utf-8")
        if "not exist" in line:
            print(f">>> ERROR: {answers['topic_name']} does not exist.")
            break
        print(line)

    result.stdout.close()


def getLog(server):
    print("Press CTRL + C to end logging.")
    config.read("pid.ini")

    if server == "kafka":
        KF_PID = config.getint("PID", "KAFKA_SERVER")
        result = subprocess.Popen(
            shlex.split(f"cat /proc/{KF_PID}/fd/1"), stdout=subprocess.PIPE,
        )
        try:
            logNo = 0
            for line in iter(result.stdout.readline, b""):
                line = line.decode("utf-8")
                print(f"{logNo}: {line}")
                logNo += 1
        except KeyboardInterrupt:
            print("Pressed CTRL+C...")
            result.stdout.close()
            return -1
    elif server == "zookeeper":
        ZK_PID = config.getint("PID", "ZOOKEEPER_SERVER")

        result = subprocess.Popen(
            shlex.split(f"cat /proc/{ZK_PID}/fd/1"), stdout=subprocess.PIPE,
        )
        logNo = 0
        try:
            for line in iter(result.stdout.readline, b""):
                line = line.decode("utf-8")
                print(f"{logNo}: {line}")
                logNo += 1
        except KeyboardInterrupt:
            print("Pressed CTRL+C...")
            result.stdout.close()
            return -1


def getParser():
    parser = argparse.ArgumentParser(description="Apache Kafka local CLI")
    parser.add_argument(
        "-zk",
        "--zkServer",
        type=str,
        default="localhost:2181",
        help="Zookeeper server ip",
    )

    parser.add_argument(
        "-kf", "--kfServer", type=str, default="localhost:9092", help="Kafka server ip",
    )
    return parser


def checkServer(answers):
    if answers.get("topic", False) != "Skip to next menu":
        return False
    return ZK_STATUS or KF_STATUS  # if choice system went wrong. (missing "topic")


def checkAnswer(answers):
    if answers.get("topic", False) == "Skip to next menu":
        if "log" in answers and answers["log"] == "Skip to next menu":
            return True
    return False


if __name__ == "__main__":
    args = getParser().parse_args()  # Arguments
    ZK_IP, ZK_PORT = args.zkServer.split(":")
    KF_IP, KF_PORT = args.kfServer.split(":")

    try:
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if sock.connect_ex((ZK_IP, int(ZK_PORT))) == 0:  # Check everytime
                ZK_STATUS = True
                print(">>> Zookeeper is connected.")
            else:
                ZK_STATUS = False
                print(">>> Zookeeper is not connected.")
            sock.close()

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if sock.connect_ex((KF_IP, int(KF_PORT))) == 0:  # Check everytime
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
                        {"name": "Description"},
                        {"name": "List"},
                        {"name": "Skip to next menu"},
                    ],
                },
                {
                    "type": "list",
                    "message": "Server logging",
                    "name": "log",
                    "choices": [
                        Separator("= Server ="),
                        {"name": "Show Zookeeper log"},
                        {"name": "Show Kafka log"},
                        {"name": "Skip to next menu"},
                    ],
                    "when": checkServer,  # at least, one server must be available
                },
                {
                    "type": "list",
                    "message": "Server options",
                    "name": "server",
                    "choices": [
                        Separator("= Server options ="),
                        {"name": "Turn off Kafka"},
                        {"name": "Turn off Zookeeper"},
                        # {"name": "Change Zookeeper ip"},
                        # {"name": "Change Kafka ip"},
                        {"name": "See menu again"},
                    ],
                    "when": checkAnswer,  # PyInquirer "when" must use a function with answers parameter.
                },
            ]

            answers = prompt(questions, style=style)
            if "server" in answers:
                if answers["server"] == "Turn off Kafka":
                    turnOff("kafka")
                elif answers["server"] == "Turn off Zookeeper":
                    turnOff("zookeeper")
                # elif answers["server"] == "Change Zookeeper ip":
                #     changeIP("zookeeper")
                # elif answers["server"] == "Change Kafka ip":
                #     changeIP("kafka")

            if "log" in answers:
                if answers["log"] == "Show Kafka log":
                    getLog("kafka")
                elif answers["log"] == "Show Zookeeper log":
                    getLog("zookeeper")

            if "topic" in answers:
                if answers["topic"] == "Create":
                    createTopic()
                elif answers["topic"] == "Delete":
                    deleteTopic()
                elif answers["topic"] == "Description":
                    getDescription()
                elif answers["topic"] == "List":
                    getTopicList()

            questions = [
                {
                    "type": "confirm",
                    "message": "= Do you want to exit the program? =",
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
