# -*- coding: utf-8 -*-

"""
工业视觉识别与MQTT通信脚本 (兼容Python 3.8)
功能：
1. 监听MQTT消息，接收拍照触发指令。
2. 控制海康威视GigE相机进行软件触发拍照。
3. 使用PaddleOCR对图像进行文字识别。
4. 将识别结果通过MQTT发布出去。
5. 具备相机通信异常自动恢复功能。
"""

# ==============================================================================
# 1. 导入库
# ==============================================================================
import re
import traceback
import time
from collections import Counter
from datetime import datetime
from queue import Queue, Empty
from ctypes import cast, POINTER
from typing import Optional  # Python 3.8 兼容性：导入 Optional

# 第三方库
import cv2
import numpy as np
from paddleocr import PaddleOCR

# 海康威视SDK
from MvImport import MvCameraControl_class as mvs

# 本地MQTT客户端
from mqtt_client import new_client

# ==============================================================================
# 2. 全局配置
# ==============================================================================
# MQTT配置
MQTT_BROKER_HOST = "localhost"  # MQTT服务器地址
MQTT_BROKER_PORT = 1883        # MQTT服务器端口
MQTT_TOPIC_SUB = "mqtt_plc/scan"  # 订阅的主题，用于接收触发信号
MQTT_TOPIC_PUB = "mqtt_plc/code"  # 发布的主题，用于发送识别结果

# 消息有效时间
TTL_SECONDS = 5  # 收到的MQTT消息超过此秒数将被视为过期

# OCR配置
OCR_USE_GPU = False
OCR_LANG = "ch"
OCR_CONFIDENCE_THRESHOLD = 0.8

# 相机配置
CAMERA_TRIGGER_TIMEOUT_MS = 3000  # 相机取图超时时间
CAMERA_MAX_FAILURES = 3            # 连续失败多少次后尝试重连

# 识别码业务规则
# True: 只返回以"LKTT"开头且长度为10的码
# False: 返回所有清理后的字母数字组合
ENABLE_CODE_FILTER = True
CODE_PREFIX = "LKTT"
CODE_LENGTH = 10
MV_ACCESS_MODE = mvs.MV_ACCESS_Exclusive
# ==============================================================================
# 3. 全局变量
# ==============================================================================
# 初始化OCR引擎
ocr = PaddleOCR(use_angle_cls=True, lang=OCR_LANG, use_gpu=OCR_USE_GPU, show_log=False)

# 相机相关
cam = mvs.MvCamera()
camera_initialized = False
consecutive_failures = 0

# MQTT和队列
mqtt_client = None
message_queue = Queue()

# ==============================================================================
# 4. 核心功能函数
# ==============================================================================

def fix_code(code: str) -> str:
    """
    清理和验证识别到的字符串。
    1. 移除非字母数字字符。
    2. 转换为大写。
    3. （可选）根据业务规则进行过滤。
    """
    if not isinstance(code, str):
        return ""
    cleaned_code = re.sub(r"[^a-zA-Z0-9]", "", code).upper()

    if ENABLE_CODE_FILTER:
        if cleaned_code.startswith(CODE_PREFIX) and len(cleaned_code) == CODE_LENGTH:
            return cleaned_code
        return ""
    
    return cleaned_code

def initialize_camera() -> bool:
    """
    初始化相机：枚举、创建句柄、打开设备、配置触发模式、开始采集。
    返回: True表示成功, False表示失败。
    """
    global cam, camera_initialized
    print("--- 开始初始化相机 ---")
    try:
        # 初始化SDK
        mvs.MvCamera.MV_CC_Initialize()
        
        # 枚举设备
        dev_list = mvs.MV_CC_DEVICE_INFO_LIST()
        ret = mvs.MvCamera.MV_CC_EnumDevices(mvs.MV_GIGE_DEVICE, dev_list)
        if ret != mvs.MV_OK or dev_list.nDeviceNum == 0:
            raise Exception(f"枚举相机失败（错误码：{ret}），未找到可用GigE相机")

        # 创建句柄并打开设备
        st_device_info = cast(dev_list.pDeviceInfo[0], POINTER(mvs.MV_CC_DEVICE_INFO)).contents
        ret = cam.MV_CC_CreateHandle(st_device_info)
        if ret != mvs.MV_OK:
            raise Exception(f"创建相机句柄失败（错误码：{ret}）")

        ret = cam.MV_CC_OpenDevice(MV_ACCESS_MODE)
        if ret != mvs.MV_OK:
            raise Exception(f"打开相机失败（错误码：{ret}）")

        # 配置为软件触发模式
        cam.MV_CC_SetEnumValue("TriggerMode", mvs.MV_TRIGGER_MODE_ON)
        cam.MV_CC_SetEnumValue("TriggerSource", mvs.MV_TRIGGER_SOURCE_SOFTWARE)
        
        # 开始采集
        ret = cam.MV_CC_StartGrabbing()
        if ret != mvs.MV_OK:
            raise Exception(f"开始采集失败（错误码：{ret}）")

        camera_initialized = True
        print("--- 相机初始化成功 ---")
        return True

    except Exception as e:
        print(f"!!! 相机初始化失败: {str(e)} !!!")
        # 清理已分配的资源
        if cam.MV_CC_IsDeviceConnected():
            cam.MV_CC_CloseDevice()
        if cam.handle:
            cam.MV_CC_DestroyHandle()
        mvs.MvCamera.MV_CC_Finalize()
        camera_initialized = False
        return False

def reconnect_camera() -> bool:
    """
    执行完整的相机重连流程。
    返回: True表示重连成功, False表示失败。
    """
    global camera_initialized
    print("\n!!! 检测到连续失败，尝试重新连接相机...")
    try:
        # 1. 停止采集并关闭设备
        if cam.MV_CC_IsDeviceConnected():
            cam.MV_CC_StopGrabbing()
            cam.MV_CC_CloseDevice()
        
        # 2. 销毁句柄
        if cam.handle:
            cam.MV_CC_DestroyHandle()

        time.sleep(1) # 等待资源释放

        # 3. 重新初始化
        return initialize_camera()

    except Exception as e:
        print(f"!!! 相机重连过程中发生异常: {str(e)} !!!")
        camera_initialized = False
        return False

def capture_and_ocr() -> Optional[str]:
    """
    执行拍照、图像处理和OCR识别。
    返回: 识别出的最终字符串，失败则返回None。
    """
    global consecutive_failures
    print("\n=== 开始拍照识别 ===")
    frame_out = None
    try:
        # 软件触发
        cam.MV_CC_SetCommandValue("TriggerSoftware")
        
        # 获取图像缓冲区
        frame_out = mvs.MV_FRAME_OUT()
        ret = cam.MV_CC_GetImageBuffer(frame_out, CAMERA_TRIGGER_TIMEOUT_MS)

        if ret != mvs.MV_OK:
            consecutive_failures += 1
            print(f"取图失败（错误码：{ret}），连续失败次数: {consecutive_failures}")
            if consecutive_failures >= CAMERA_MAX_FAILURES:
                if reconnect_camera():
                    consecutive_failures = 0  # 重连成功，重置计数器
                else:
                    print("!!! 重连失败，程序可能无法继续正常工作。")
            return None

        # 取图成功，重置失败计数器
        consecutive_failures = 0

        # --- 图像处理 ---
        frame_info = frame_out.stFrameInfo
        image_data = np.frombuffer(
            mvs.string_at(frame_out.pBufAddr, frame_info.nFrameLen), dtype=np.uint8
        ).reshape(frame_info.nHeight, frame_info.nWidth)

        # 转为灰度图
        gray_image = cv2.cvtColor(image_data, cv2.COLOR_BGR2GRAY) if len(image_data.shape) == 3 else image_data

        # --- OCR识别 ---
        results = []
        ocr_results = ocr.ocr(gray_image, cls=True)
        if ocr_results:
            for line in ocr_results:
                if line is None:
                    continue
                for word_info in line:
                    text = word_info[1][0]
                    confidence = word_info[1][1]
                    if confidence > OCR_CONFIDENCE_THRESHOLD:
                        cleaned_text = fix_code(text)
                        if cleaned_text:
                            results.append(cleaned_text)
        
        if not results:
            print("未识别到任何有效结果")
            # 保存图像
            
            return None

        # 统计最高频结果
        most_common_result, _ = Counter(results).most_common(1)[0]
        print(f"识别成功，结果: 「{most_common_result}」")
        return most_common_result

    except Exception as e:
        print(f"拍照识别过程异常：{str(e)}")
        traceback.print_exc()
        return None
    finally:
        # 【关键修复】仅在成功获取图像后释放缓冲区
        if frame_out is not None and 'ret' in locals() and ret == mvs.MV_OK:
            cam.MV_CC_FreeImageBuffer(frame_out)
        print("=== 拍照识别流程结束 ===")


def on_mqtt_message(client, userdata, msg):
    """
    MQTT消息回调函数：接收触发信号并放入队列。
    """
    try:
        payload = msg.payload.decode("utf-8").strip()
        print(f"\n收到MQTT消息: 主题={msg.topic}, 内容={payload}")

        time_format = "%Y-%m-%d %H:%M:%S"
        target_time = datetime.strptime(payload, time_format)
        
        # 检查消息是否过期
        if (datetime.now() - target_time).total_seconds() > TTL_SECONDS:
            print("消息已过期，忽略。")
            return
            
        message_queue.put(target_time)
        print(f"消息已加入队列，当前队列长度: {message_queue.qsize()}")

    except ValueError:
        print(f"MQTT消息格式错误，忽略: {payload}")
    except Exception as e:
        print(f"处理MQTT消息时发生异常: {str(e)}")


# ==============================================================================
# 5. 主程序入口
# ==============================================================================

if __name__ == "__main__":
    # 初始化相机
    if not initialize_camera():
        print("相机初始化失败，程序退出。")
        exit(1)

    # 初始化MQTT客户端
    try:
        mqtt_client = new_client()
        mqtt_client.on_message = on_mqtt_message
        mqtt_client.loop_start()
    except Exception as e:
        print(f"MQTT客户端启动失败: {str(e)}")
        # 退出前释放相机资源
        if camera_initialized:
            cam.MV_CC_StopGrabbing()
            cam.MV_CC_CloseDevice()
            cam.MV_CC_DestroyHandle()
            mvs.MvCamera.MV_CC_Finalize()
        exit(1)

    # 主循环
    try:
        while True:
            try:
                # 从队列获取消息，超时1秒以便能响应KeyboardInterrupt
                target_time = message_queue.get(timeout=1)
                
                # 再次检查消息是否过期（防止在队列中等待过久）
                if (datetime.now() - target_time).total_seconds() > TTL_SECONDS:
                    print("队列中的消息已过期，忽略。")
                    continue
                
                # 执行识别任务
                final_result = capture_and_ocr()
                if final_result:
                    mqtt_client.publish(MQTT_TOPIC_PUB, final_result)
                    print(f"识别结果已发布到主题 {MQTT_TOPIC_PUB}")

            except Empty:
                continue # 队列为空，继续循环
            except Exception as e:
                print(f"主循环发生未知异常: {str(e)}")
                traceback.print_exc()

    except KeyboardInterrupt:
        print("\n程序被用户中断，正在清理资源...")
    finally:
        print("--- 开始清理资源 ---")
        # 释放相机资源
        if camera_initialized:
            print("正在停止相机采集...")
            cam.MV_CC_StopGrabbing()
            print("正在关闭相机设备...")
            cam.MV_CC_CloseDevice()
            print("正在销毁相机句柄...")
            cam.MV_CC_DestroyHandle()
            mvs.MvCamera.MV_CC_Finalize()
            print("相机资源已释放。")
        
        # 停止MQTT客户端
        if mqtt_client:
            print("正在停止MQTT客户端...")
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            print("MQTT客户端已停止。")
        print("--- 所有资源清理完毕，程序退出 ---")
