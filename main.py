import contextlib
import math
from typing import Dict, List, Set, Union
import serial
import os
import traceback
import struct
import time
import json
import decimal
import base64
import threading
import requests
import random
import queue
import logging
from datetime import datetime, timedelta
import npyscreen

version = '4.3'

log_filename = 'hub.log'
logging.basicConfig(filename=log_filename, level=logging.DEBUG)

endpoint = 'https://doe-building-america.cu-isee.org/sensors'
key = 'PB6bMDXk99jMCK35qkdgNVc7dKAGLFNaFPy5LrYuyYmPNGnnX6RM3eJ9ZcbrTRdG'

debug = False
update_remote = False

stop_signal = threading.Event()

pusher = None
sensors = dict()

# Helpers
def clamp(n, minn, maxn):
    return max(min(maxn, n), minn)

class ConnectivityStatusIndicator:
    def __init__(self, endpoint, interval):
        self.endpoint = endpoint
        self.interval = interval

class CloudEndpoint():
    def __init__(self, endpoint, key, interval=10):
        self.endpoint = endpoint
        self.key = key
        self.push_interval = interval
        self.submission_queues = dict()
        self.queue_lock = threading.Lock()
        self.submission_thread = None
        self.running = threading.Event()
        self.queued = 0
        self.pushed = 0

    def submit(self, sensor_id, timestamp, readings):
        self.queue_lock.acquire()
        if sensor_id not in self.submission_queues:
            self.submission_queues[sensor_id] = queue.PriorityQueue()
        
        self.submission_queues[sensor_id].put((timestamp, readings))
        self.queue_lock.release()
        self.queued += 1
        current_status[3] = 'Queued/Pushed: {0}/{1}'.format(self.queued, self.pushed)

    def start(self):
        self.submission_thread = threading.Thread(target=self.push)
        self.submission_thread.start()

    def push(self):
        while not stop_signal.is_set():
            try:
                if self.running.is_set():
                    logging.debug('Skip pushing, running: {0}, pausing: {1}'.format(self.running.is_set(), self.pausing.is_set()))
                    return
                self.running.set()
                self.queue_lock.acquire()
                for sensor_id in self.submission_queues:
                    records = list()
                    while not self.submission_queues[sensor_id].empty():
                        timestamp, readings = self.submission_queues[sensor_id].get()
                        records.append({'timestamp': timestamp, 'value': readings})
                    data = {'key': self.key, 'records': records}
                    response = requests.post(self.endpoint + '/' + sensor_id + '/data', json=data)
                    if response.ok:
                        update_data = response.json()
                        if not update_remote:
                            sensors[sensor_id].update_config(update_data)
                        self.queued -= len(records)
                        self.pushed += len(records)
                        current_status[3] = 'Queued/Pushed: {0}/{1}'.format(self.queued, self.pushed)
                        current_status[4] = ''
                    else:
                        for record in records:
                            self.submission_queues[sensor_id].put((record['timestamp'], record['value']))
                
            except Exception as e:
                logging.exception('[Push]')
                current_status[4] = 'Internet Connection Lost'
            finally:
                self.queue_lock.release()
                self.running.clear()
            time.sleep(self.push_interval)
        try:
            save_config(sensors)
        except Exception as e:
            logging.exception('[Config updating]')


class LoRaTHSensor:
    def __init__(self, sensor_id, sensor_name, report_interval, data_handler):
        self.sensor_id = sensor_id
        self.sensor_name = sensor_name
        self.sensor_type = 'LoRaTH'
        self.report_interval = report_interval
        self.data_handler = data_handler
        self.reading_display = '-'
        self.last_reading_timestamp = 0
        self.last_push_timestamp = 0
        self.pushed_readings = 0

    def receive(self, timestamp, rssi, snr, signal_rssi, sid, temperature, humidity, extension, battery, checksum):
        self.reading_display = '{0:.2f}°C {1:.1f}%RH         {2:.3f}V '.format(temperature, humidity, battery / 1000)
        self.last_reading_timestamp = timestamp
        # Discard readings based on report interval
        if timestamp < self.last_push_timestamp + self.report_interval * 0.8:
            return
        self.data_handler.submit(self.sensor_id, timestamp, {
            'temperature': temperature,
            'humidity': humidity,
            'battery': battery
        })
        self.pushed_readings += 1
        self.last_push_timestamp = timestamp

    def update_config(self, update_data):
        self.sensor_name = update_data['sensor-name']
        self.report_interval = update_data['report-interval']
            
class LoRaTHASensor:
    def __init__(self, sensor_id, sensor_name, report_interval, data_handler):
        self.sensor_id = sensor_id
        self.sensor_name = sensor_name
        self.sensor_type = 'LoRaTHA'
        self.report_interval = report_interval
        self.data_handler = data_handler
        self.reading_display = '-'
        self.last_reading_timestamp = 0
        self.last_push_timestamp = 0
        self.pushed_readings = 0

    airflow_conversion_low = {'3.5': 0.3, '3.6': 0.31, '3.7': 0.31, '3.8': 0.32, '3.9': 0.33, '4.0': 0.34, '4.1': 0.35, '4.2': 0.36, '4.3': 0.37, '4.4': 0.37, 
    '4.5': 0.38, '4.6': 0.39, '4.7': 0.4, '4.8': 0.41, '4.9': 0.42, '5.0': 0.43, '5.1': 0.43, '5.2': 0.44, '5.3': 0.5, '5.4': 0.51, '5.5': 0.52, '5.6': 0.53, 
    '5.7': 0.54, '5.8': 0.55, '5.9': 0.56, '6.0': 0.57, '6.1': 0.58, '6.2': 0.59, '6.3': 0.6, '6.4': 0.61, '6.5': 0.62, '6.6': 0.62, '6.7': 0.63, '6.8': 0.65, 
    '6.9': 0.66, '7.0': 0.8, '7.1': 0.81, '7.2': 0.82, '7.3': 0.83, '7.4': 0.84, '7.5': 0.86, '7.6': 0.87, '7.7': 0.88, '7.8': 0.89, '7.9': 0.9, '8.0': 0.91, 
    '8.1': 0.93, '8.2': 0.94, '8.3': 0.95, '8.4': 0.96, '8.5': 0.97, '8.6': 0.98, '8.7': 0.99, '8.8': 1, '8.9': 1.02, '9.0': 1.03, '9.1': 1.04, '9.2': 1.05, 
    '9.3': 1.06, '9.4': 1.07, '9.5': 1.09, '9.6': 1.1, '9.7': 1.1, '9.8': 1.12, '9.9': 1.13, '10.0': 1.14, '10.1': 1.15, '10.2': 1.17, '10.3': 1.18, '10.4': 1.19, 
    '10.5': 1.1, '10.6': 1.12, '10.7': 1.12, '10.8': 1.13, '10.9': 1.14, '11.0': 1.15, '11.1': 1.16, '11.2': 1.17, '11.3': 1.19, '11.4': 1.2, '11.5': 1.21, 
    '11.6': 1.22, '11.7': 1.22, '11.8': 1.23, '11.9': 1.24, '12.0': 1.25, '12.1': 1.27, '12.2': 1.24, '12.3': 1.2, '12.4': 1.21, '12.5': 1.22, '12.6': 1.24, 
    '12.7': 1.25, '12.8': 1.26, '12.9': 1.27, '13.0': 1.28, '13.1': 1.29, '13.2': 1.3, '13.3': 1.31, '13.4': 1.32, '13.5': 1.33, '13.6': 1.34, '13.7': 1.34, 
    '13.8': 1.35, '13.9': 1.36, '14.0': 1.2, '14.1': 1.21, '14.2': 1.22, '14.3': 1.23, '14.4': 1.24, '14.5': 1.24, '14.6': 1.26, '14.7': 1.27, '14.8': 1.27, 
    '14.9': 1.29, '15.0': 1.29, '15.1': 1.3, '15.2': 1.31, '15.3': 1.32, '15.4': 1.32, '15.5': 1.33, '15.6': 1.34, '15.7': 1.32, '15.8': 1.3, '15.9': 1.31, 
    '16.0': 1.32, '16.1': 1.33, '16.2': 1.33, '16.3': 1.34, '16.4': 1.35, '16.5': 1.36, '16.6': 1.37, '16.7': 1.38, '16.8': 1.38, '16.9': 1.39, '17.0': 1.4, 
    '17.1': 1.42, '17.2': 1.42, '17.3': 1.43, '17.4': 1.45, '17.5': 1.4, '17.6': 1.41, '17.7': 1.42, '17.8': 1.43, '17.9': 1.44, '18.0': 1.45, '18.1': 1.46, 
    '18.2': 1.46, '18.3': 1.47, '18.4': 1.47, '18.5': 1.48, '18.6': 1.49, '18.7': 1.5, '18.8': 1.5, '18.9': 1.51, '19.0': 1.52, '19.1': 1.53, '19.2': 1.43, 
    '19.3': 1.41, '19.4': 1.42, '19.5': 1.42, '19.6': 1.43, '19.7': 1.44, '19.8': 1.45, '19.9': 1.45, '20.0': 1.46}

    airflow_conversion_high = {'20': 1.46, '22': 1.57, '24': 1.6, '26': 1.7, '28': 1.81, '30': 1.92, '32': 2.04, '34': 2.16, '36': 2.26, '38': 2.4, '40': 2.5, 
    '42': 2.61, '44': 2.72, '46': 2.85, '48': 2.86, '50': 2.97, '52': 3.09, '54': 3.21, '56': 3.32, '58': 3.43, '60': 3.55, '62': 3.66, '64': 3.77, '66': 3.88, 
    '68': 4.01, '70': 4.12, '72': 4.22, '74': 4.36, '76': 4.49, '78': 4.58, '80': 4.59, '82': 4.71, '84': 4.83, '86': 4.94, '88': 5.07, '90': 5.17, '92': 5.29, 
    '94': 5.4, '96': 5.53, '98': 5.63, '100': 5.76}

    def receive(self, timestamp, rssi, snr, signal_rssi, sid, temperature, humidity, airflow, battery, checksum):
        airflow = self.convert_airflow(airflow)
        self.reading_display = '{0:.2f}°C {1:.1f}%RH {2:.2f}m/s {3:.3f}V '.format(temperature, humidity, airflow, battery / 1000)
        self.last_reading_timestamp = timestamp
        # Discard readings based on report interval
        if timestamp < self.last_push_timestamp + self.report_interval * 0.8:
            return
        self.data_handler.submit(self.sensor_id, timestamp, {
            'temperature': temperature,
            'humidity': humidity,
            'airflow': airflow,
            'battery': battery
        })
        self.pushed_readings += 1
        self.last_push_timestamp = timestamp
        

    def update_config(self, update_data):
        self.sensor_name = update_data['sensor-name']
        self.report_interval = update_data['report-interval']

    def convert_airflow(self, airflow_freq):
        if airflow_freq < 3.5: return 0.
        if airflow_freq < 20:
            freq_low = math.floor(airflow_freq * 10) / 10
            freq_high = freq_low + 0.1
            freq_str_low = '{0:.1f}'.format(freq_low)
            freq_str_high = '{0:.1f}'.format(freq_high)
            velo_low = LoRaTHASensor.airflow_conversion_low[freq_str_low]
            velo_high = LoRaTHASensor.airflow_conversion_low[freq_str_high]
            airflow_velo = (velo_high - velo_low) * (airflow_freq - freq_low) / 0.1 + velo_low
            return airflow_velo
        if airflow_freq <= 100:
            freq_low = math.floor(airflow_freq // 2 * 2)
            freq_high = freq_low + 2
            freq_str_low = '{0:.0f}'.format(freq_low)
            freq_str_high = '{0:.0f}'.format(freq_high)
            velo_low = LoRaTHASensor.airflow_conversion_high[freq_str_low]
            velo_high = LoRaTHASensor.airflow_conversion_high[freq_str_high]
            airflow_velo = (velo_high - velo_low) * (airflow_freq - freq_low) / 2 + velo_low
            return airflow_velo
        airflow_velo = 0.0532 * airflow_freq + 0.3714
        return airflow_velo
        

class LoRaTHOSensor:
    def __init__(self, sensor_id, sensor_name, report_interval, data_handler):
        self.sensor_id = sensor_id
        self.sensor_name = sensor_name
        self.sensor_type = 'LoRaTHO'
        self.report_interval = report_interval
        self.data_handler = data_handler
        self.pushed_readings = 0

        self.motion_event = 0

        self.reading_display = '-'
        self.last_reading_timestamp = 0
        self.last_push_timestamp = 0

    def receive(self, timestamp, rssi, snr, signal_rssi, sid, temperature, humidity, motion, battery, checksum):   
        if motion < 0:
            self.motion_event += 1     
        self.reading_display = '{0:.2f}°C {1:.1f}%RH {2} times {3:.3f}V '.format(temperature, humidity, self.motion_event, battery / 1000)
        self.last_reading_timestamp = timestamp
        # Discard readings based on report interval
        if timestamp < self.last_push_timestamp + self.report_interval * 0.8:
            return
        self.data_handler.submit(self.sensor_id, timestamp, {
            'temperature': temperature,
            'humidity': humidity,
            'occupancy': self.motion_event,
            'battery': battery
        })
        self.motion_event = 0
        self.pushed_readings += 1
        self.last_push_timestamp = timestamp

    def update_config(self, update_data):
        self.sensor_name = update_data['sensor-name']
        self.report_interval = update_data['report-interval']



def handle_dataframe(frame: Union[List[bytes], bytes], timestamp: int):
    if isinstance(frame, list):
        frame = b''.join(frame)
    if debug:
        log = open('frame_dump.log', 'a')
        log.write(frame.hex() + '\n')
        log.close()
    if len(frame) != 30:
        statistic['bad_frames'] += 1
        current_status[2] = 'Rejected: {0}'.format(statistic['bad_frames'])
        return
    if sum(frame[3:29]) % 256 == frame[29]:
        rssi, snr, signalr_rssi, sid = struct.unpack('>BBB12s', frame[0:15])
        temperature, humidity, airflow, battery, checksum = struct.unpack('>fffHB', frame[15:30])
        sid = base64.b32encode(sid).decode('utf-8').replace('=', '0')
        if sid not in sensors or sensors[sid].pushed_readings == 0:
            handle_unknown_sensor(sid)
        sensors[sid].receive(timestamp, rssi, snr, signalr_rssi, sid, temperature, humidity, airflow, battery,
                                   checksum)
        statistic['processed_frames'] += 1
        current_status[1] = 'Processed: {0}'.format(statistic['processed_frames'])
    else:
        statistic['bad_frames'] += 1
        current_status[2] = 'Rejected: {0}'.format(statistic['bad_frames'])

def pull_remote_sensor_info(response):
    sensor_info = response.json()
    sid = sensor_info['sensor-id']
    sname = sensor_info['sensor-name']
    stype = sensor_info['sensor-type']
    interval = sensor_info['report-interval']
    if stype == 'LoRaTHO':
        sensor = LoRaTHOSensor(sid, sname, interval, pusher)
    elif stype == 'LoRaTHA':
        sensor = LoRaTHASensor(sid, sname, interval, pusher)
    else:
        sensor = LoRaTHSensor(sid, sname, interval, pusher)
    sensors[sid] = sensor
    save_config(sensors)

def update_remote_sensor_info(sensor):
    unit = {'temperature': 'degC', 'humidity': '%RH'}
    if sensor.sensor_type == 'LoRaTHA':
        unit['airflow'] = 'm/s'
    elif sensor.sensor_type == 'LoRaTHO':
        unit['occupancy'] = '/min'
    data = {
        'sensor-name': sensor.sensor_name,
        'sensor-type': sensor.sensor_type,
        'report-interval': sensor.report_interval,
        'unit': unit,
        'key': key
    }
    response = requests.put(endpoint + '/' + sensor.sensor_id + '/info', json=data)

def create_new_sensor_placeholder(sid):
    sensor = LoRaTHSensor(sid, 'Unkn-' + sid[0:5], 60, pusher)
    sensors[sid] = sensor
    return sensor

def register_sensor(sensor):
    current_status[0] = 'Registering New Sensor'
    unit = {'temperature': 'degC', 'humidity': '%RH'}
    if sensor.sensor_type == 'LoRaTHA':
        unit['airflow'] = 'm/s'
    elif sensor.sensor_type == 'LoRaTHO':
        unit['occupancy'] = '/min'
    new_sensor_info = {
        'sensor-id': sensor.sensor_id,
        'sensor-name': sensor.sensor_name,
        'sensor-type': sensor.sensor_type,
        'report-interval': sensor.report_interval,
        'unit': unit,
        'key': key
    }
    response = requests.post(endpoint, json=new_sensor_info)
    if response.status_code == requests.codes.created:
        response = requests.get(endpoint + '/' + sensor.sensor_id + '/info', params={'key': key})
        return response.json()
    else:
        # print(response.status_code)
        raise Exception(response.status_code)



def handle_unknown_sensor(sid: str):
    response = requests.get(endpoint + '/' + sid + '/info', params={'key': key})
    if response.status_code == requests.codes.not_found:
        if sid in sensors:
            register_sensor(sensors[sid])
        else:
            sensor = create_new_sensor_placeholder(sid)
            register_sensor(sensor)
    else:
        if sid in sensors:
            if update_remote:
                update_remote_sensor_info(sensors[sid])
        else:
            pull_remote_sensor_info(response)


def collect_sensor_data(port: str = '/dev/ttyS0', baud_rate: int = 19200):
    with serial.Serial(port, baudrate=baud_rate) as ser:
        while not stop_signal.isSet():
            try:
                current_status[0] = 'Running'
                b = ser.read(1)
                serial_buffer.put((b, datetime.now()))
            except Exception as e:
                logging.exception('[collect_sensor_data]')


def identify_data_frame():
    ascii_buffer = list()
    current_byte = b''
    while not stop_signal.isSet():
        try:
            last_byte = current_byte
            current_byte, current_timestamp = serial_buffer.get()
            ascii_buffer.append(current_byte)

            if debug:
                statistic['bytes'] += 1
                current_status[5] = 'ReceivedBytes ' + str(statistic['bytes'])
                log = open('byte_dump.log', 'a')
                log.write(current_byte.hex() + ' ')
                log.close()

            if current_byte == b'\x0a' and last_byte == b'\x0d':

                if debug:
                    statistic['identified'] += 1
                    current_status[6] = 'Identified: ' + str(statistic['identified'])

                buffer = list()
                for i in range(0, 30):
                    bh = int.from_bytes(ascii_buffer[i * 2], byteorder='big') - 48
                    bl = int.from_bytes(ascii_buffer[i * 2 + 1], byteorder='big') - 48
                    bh = bh - 7 if bh > 10 else bh
                    bl = bl - 7 if bl > 10 else bl
                    buffer.append(bh * 16 + bl)
                try:
                    byte_buffer = bytes(buffer)
                except Exception as e:
                    logging.exception('[identify_data_frame]-Converting Bytes')
                    ascii_buffer.clear()
                handle_dataframe(byte_buffer, int(current_timestamp.timestamp()))
                ascii_buffer.clear()

        except Exception as e:
            logging.exception('[identify_data_frame]')




# Console GUI

# This application class serves as a wrapper for the initialization of curses
# and also manages the actual forms of the application
class HubApp(npyscreen.NPSAppManaged):
    def onStart(self):
        self.registerForm("MAIN", MainAppForm())

    def onCleanExit(self):
        stop_signal.set()


# This form class defines the display that will be presented to the user.
class MainAppForm(npyscreen.Form):
    def create(self):
        self.update_thread = None
        self.update_stop_signal = threading.Event()

        self.title = self.add(npyscreen.TitleFixedText, name='Sensor Hub System v{0}'.format(version))
        self.status = self.add(npyscreen.TitleFixedText, name='Status', value='initialized')
        self.clock = self.add(npyscreen.TitleFixedText, name='Current Time', value='')

        self.add(npyscreen.TitleFixedText, name=' ')

        self.add(npyscreen.TitleFixedText, name=' ', value='Sensor      Pushed    Current Reading')

        self.sensor_list = self.add(npyscreen.TitlePager, name='Sensors', values=[])
        self.sensor_index = dict()

        self.start_update()

    def afterEditing(self):
        self.stop_update()
        self.parentApp.setNextForm(None)

    def set_status(self, status: str):
        self.status.value = status
        self.clock.display()

    def start_update(self):
        self.update_thread = threading.Thread(target=self.update)
        self.update_thread.start()

    def stop_update(self):
        global stop_signal
        stop_signal.set()
        self.update_stop_signal.set()

    def update(self):
        redraw_counter = 0
        while True:
            if self.update_stop_signal.isSet():
                return
            self.status.value = '  '.join(current_status)
            self.status.display()

            now = datetime.now()
            self.clock.value = datetime.strftime(now, "%Y-%m-%d %H:%M:%S")
            self.clock.display()

            for sid in sensors:
                if sid not in self.sensor_index:
                    self.sensor_index[sid] = len(self.sensor_index)
                    self.sensor_list.values.append('')
                index = self.sensor_index[sid]

                if sensors[sid].last_reading_timestamp > 0:
                    self.sensor_list.values[index] = '{0:<10}  {1:<6}    {2} {3} ({4})'.format(
                        sensors[sid].sensor_name[0:10],
                        sensors[sid].pushed_readings,
                        sensors[sid].reading_display,
                        datetime.fromtimestamp(sensors[sid].last_reading_timestamp),
                        'P' if sensors[sid].last_reading_timestamp == sensors[sid].last_push_timestamp else 'D'
                    )
                else:
                    self.sensor_list.values[index] = '{0:<10}  {1:<6}    {2}'.format(
                        sensors[sid].sensor_name[0:10],
                        sensors[sid].pushed_readings,
                        sensors[sid].reading_display)
            self.sensor_list.display()

            time.sleep(1)
            if redraw_counter % 10 == 0:
                redraw_counter = 0
                self.DISPLAY()
            redraw_counter += 1

def load_config():
    with open('config.json', 'r') as fp:
        config = json.load(fp)
    endpoint = config['endpoint']
    key = config['key']

    update_remote = config['update_remote_config']

    pusher = CloudEndpoint(endpoint, key)

    sensors = dict()

    for sconf in config['sensors']:
        sid = sconf['id']
        sname = sconf['name']
        stype = sconf['type']
        interval = sconf['report-interval']
        if stype == 'LoRaTHO':
            sensor = LoRaTHOSensor(sid, sname, interval, pusher)
        elif stype == 'LoRaTHA':
            sensor = LoRaTHASensor(sid, sname, interval, pusher)
        else:
            sensor = LoRaTHSensor(sid, sname, interval, pusher)
        sensors[sid] = sensor

    return endpoint, key, pusher, sensors, update_remote



def save_config(sensors):
    with open('config.json', 'r') as fp:
        config = json.load(fp)
    config['update_remote_config'] = False
    config['sensors'].clear()
    for sensor in sensors.values():
        config['sensors'].append({
            'id': sensor.sensor_id,
            'name': sensor.sensor_name,
            'type': sensor.sensor_type,
            'report-interval': sensor.report_interval
        })
    with open('config.json', 'w') as fp:
        json.dump(config, fp)


if os.path.exists('./hub.lock'):
    print('Hub is already running in another terminal.')
    exit()
else:
    open('./hub.lock', 'x').close()



sensors = {}  # sensor-id to LoRaTHASensor instance

serial_buffer = queue.Queue()

current_status = ['Initializing', '', '', '', '', '', '', '', '', '', '', '']

statistic = {
    'processed_frames': 0,
    'identified': 0,
    'invalid_frame_size': 0,
    'frame_checksum_failed': 0,
    'bad_frames': 0,
    'bytes': 0
}

debug_output = ''


serial_thread = threading.Thread(target=collect_sensor_data)
serial_thread.start()

data_identify_thread = threading.Thread(target=identify_data_frame)
data_identify_thread.start()

endpoint, key, pusher, sensors, update_remote = load_config()

pusher.start()

gui_app = HubApp()
try:
    gui_app.run()
finally:
    stop_signal.set()
    serial_thread.join()
    data_identify_thread.join()
    os.remove('./hub.lock')

# generate_simulation_data()
