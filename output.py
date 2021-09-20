import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer
import time
import threading
plt_val_x=[]
plt_val_y=[]

def animate(i):
    global plt_val_x, plt_val_y

    plt.cla()

    plt.plot(plt_val_x, plt_val_y, label="BTC- USD")

    plt.legend(loc='upper left')
    plt.tight_layout()
    plt.show()

plt.style.use('fivethirtyeight')

def plot():
    for message in consumer:
        plt_val_x.append(int(time.strftime("%M", time.localtime())+time.strftime("%S", time.localtime())))
        plt_val_y.append(float(message.value["value"]))
        time.sleep(1)

consumer = KafkaConsumer('output',bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: json.loads(x.decode('utf-8')))

plot_thread = threading.Thread(target=plot)
plot_thread.start()

time.sleep(5)
ani = FuncAnimation(plt.gcf(), animate, interval=1000)  
plt.tight_layout()
plt.show()





