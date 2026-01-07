import paho.mqtt.client as mqtt
import time

# ------------------- 配置参数 -------------------
MQTT_BROKER = "xs.jldg.com"  # 公共 MQTT 服务器（无需注册，可直接使用）
MQTT_PORT = 1883  # MQTT 基础端口（无加密）
MQTT_CLIENT_ID = "ocr_client_" + str(time.time())  # 客户端 ID（需唯一，避免冲突）
MQTT_TOPIC = "mqtt_plc"  # 要发布/订阅的主题
SCAN_TOPIC = f"{MQTT_TOPIC}/scan"
CODE_TOPIC = f"{MQTT_TOPIC}/code"


# ------------------- 回调函数 -------------------
# 1. 连接成功回调
def on_connect(client, userdata, flags, rc, properties=None):
    print("连接 MQTT 服务器成功")
    # 连接成功后订阅主题
    client.subscribe(SCAN_TOPIC, qos=0)
    print(f"已订阅订阅主题：{SCAN_TOPIC}")


# 2. 接收消息回调
def on_message(client, userdata, msg, properties=None):
    print("\n收到消息：")
    print("  主题：%s" % msg.topic)
    print("  内容：%s" % msg.payload.decode("utf-8"))
    print("  QoS：%d" % msg.qos)


# 3. 发布消息成功回调
def on_publish(client, userdata, mid, properties=None):
    print("发布消息成功，消息 ID：", mid)


def new_client():
    # ------------------- 创建客户端并配置 -------------------
    # 创建 MQTT 客户端，指定使用 MQTT v5 协议
    client = mqtt.Client(
        client_id=MQTT_CLIENT_ID,
    )

    # 绑定回调函数
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish

    # 连接 MQTT 服务器（无认证）
    client.connect(MQTT_BROKER, port=MQTT_PORT, keepalive=60)
    return client


def main():
    client = new_client()

    # ------------------- 循环处理消息 -------------------
    try:
        # 启动循环（非阻塞模式，定期检查消息队列）
        client.loop_start()

        while True:
            # 读取点位数据
            client.publish(CODE_TOPIC, "123456")
            time.sleep(2)  # 间隔 5 秒

    except KeyboardInterrupt:
        # 按下 Ctrl+C 停止客户端
        print("\n正在断开 MQTT 连接...")
        client.loop_stop()  # 停止循环
        client.disconnect()  # 断开连接
        print("连接已断开")


if __name__ == "__main__":
    main()
