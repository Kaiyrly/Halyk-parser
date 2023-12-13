import json
import logging
import os
import queue
import re
import shutil
import sys
import threading
import time
import zipfile

import pandas as pd
from flask import Flask, render_template, request, make_response, redirect, url_for
from websocket_server import WebsocketServer

import main
from main import get_products

wss_values = queue.Queue()

if sys.platform == "linux" or sys.platform == "linux2":
    os.chdir("/root/Halyk/static")
    wss_ip = "185.116.193.113:5678"
else:
    os.chdir("static")
    wss_ip = "127.0.0.1:5678"

app = Flask(__name__, static_folder='static', static_url_path='')
cats = {
    "5": "Фото и видео",
    "1": "Телефоны и гаджеты",
    "2": "Ноутбуки и компьютеры",
    "3": "Кухонная техника",
    "4": "Техника для дома",
    "6": "Телевизоры и аудиотехника",
    "7": "Автотовары",
    "8": "Красота и здоровье",
    "9": "Детские товары",
    "10": "Аксессуары",
    "11": "Товары для дома и дачи",
    "12": "Спорт и отдых",
    "14": "Товары для животных",
    "31068": "Строительство и ремонт",
    "32569": "Украшения",
    "32626": "Мебель",
    "33119": "Досуг и творчество",
    "33819": "Аптека",
    "34319": "Специальные предложения",
    "34672": "Одежда",
    "34729": "Подарки, товары для праздников и цветы",
    "35891": "Halyk shop",
    "37072": "Обувь",
    "38133": "Канцелярские товары",
    "39922": "Цифровые товары"
}

tasks = {"rubr": {}, "checker": {}, "main": {}, "shop": {}}
pool = queue.Queue()


def new_client(_, server):
    server.send_message_to_all(json.dumps({"status": "init", "tasks": tasks}))


def new_message(client, server, message):
    message = json.loads(message)
    if message.get('command') == "del":
        del tasks[message['from']][str(message['id'])]
        shutil.rmtree(message['file'].replace("xlsx", "*"))
        new_client(None, server)
    elif message.get('command') == "stop":
        del tasks[message['from']][str(message['id'])]
        new_client(None, server)
    elif message.get('command') == "del_all":
        tasks.update({message['command']['from']: {}})
        new_client(None, server)
    elif message.get('command') == "download":
        with zipfile.ZipFile("results.zip", mode="w") as archive:
            for dirname, subdirs, files in os.walk("."):
                if dirname == "assets":
                    continue
                archive.write(dirname)
                for filename in files:
                    if re.search("result.*", filename) and filename != "result_store.zip":
                        archive.write(os.path.join(dirname, filename))
        server.send_message_to_all(json.dumps({"link": "results.zip"}))


def wss():
    server = WebsocketServer(host=wss_ip[:-5], port=5678, loglevel=logging.INFO)
    server.set_fn_new_client(new_client)
    server.set_fn_message_received(new_message)
    threading.Thread(target=server.run_forever).start()
    itter = 0
    while True:
        if server.clients:
            val = wss_values.get()
            val_js = json.loads(val)
            if str(val_js['id']) in tasks[val_js['type']]:
                tasks[val_js['type']][str(val_js['id'])] = val_js
            server.send_message_to_all(str(val))
            itter += 1
            if itter == 20:
                itter = 0
                new_client(None, server)


def ThreadPool():
    working = []
    while True:
        if len(working) < 5:
            tt = pool.get()
            tt.start()
            working.append(tt)
        else:
            for x in [x for x in working if not x.is_alive()]:
                working.remove(x)
        time.sleep(1)


@app.route("/login", methods=["POST", "GET"])
def login():
    if request.method == "POST":
        if request.form['log'] == "admin" and request.form['pwd'] == "kaspi_parser":
            res = make_response("")
            res.set_cookie("login", "admin", 60 * 60 * 24 * 15)
            res.headers['location'] = "/"
            return res, 302
        else:
            return render_template("login.html", alert="<script>alert('Неверный логин или пароль')</script>")
    else:
        return render_template("login.html", alert="")


@app.route("/threads")
@app.route("/")
def thread():
    if not request.cookies.get('login'):
        return redirect(url_for('login'))
    return render_template("threads.html", wss_ip=wss_ip)


@app.route("/store")
def store():
    if not request.cookies.get('login'):
        return redirect(url_for('login'))
    return render_template("store.html", wss_ip=wss_ip)


@app.route("/checker")
def checker():
    if not request.cookies.get('login'):
        return redirect(url_for('login'))
    return render_template("checker.html", wss_ip=wss_ip)


@app.route("/rubricator")
def rubricator():
    if not request.cookies.get('login'):
        return redirect(url_for('login'))
    return render_template("rubricator.html", wss_ip=wss_ip)


@app.route("/thread")
def threads():
    if not request.cookies.get('login'):
        return "<script>location.href='/login'</script>"
    f = re.sub("\n+", "\n", open("prox").read())
    return render_template("thread.html", prox=f)


@app.route("/add", methods=["POST"])
def add():
    city = request.form['city']
    proxy = request.form['proxy']
    cat = request.form['cat']
    urls = request.form['urls']
    pictures = request.form.get('pictures', None)
    ind = int(time.time())
    redirect_url = "/"
    if urls:
        for url in f"{urls}\n".split("\n"):
            ind += 1
            if url:
                if "reviews/merchant" in url:
                    task_name = "shop"
                    func = main.store_parser
                    redirect_url = "/store"
                    tasks[task_name][str(ind)] = {
                        "curr": 0,
                        "total": 0,
                        "eta": "-:-",
                        "id": ind,
                        "status": "Очередь",
                        "name": cats.get(cat, url),
                        "city": main.cities[city],
                    }
                else:
                    task_name = "main"
                    func = get_products
                    redirect_url = "/rubricator" if task_name == "rubr" else "/"
                    tasks[task_name][str(ind)] = {
                        "curr": 0,
                        "total": 0,
                        "eta": "-:-",
                        "id": ind,
                        "status": "Очередь",
                        "name": cats.get(cat, url),
                        "city": main.cities[city],
                    }
                tt = threading.Thread(target=func, args=(
                    task_name, cats.get(cat, url), cat, city, wss_values, proxy, ind, None, pictures))
    else:
        task_name = "rubr"
        func = get_products
        redirect_url = "/rubricator" if task_name == "rubr" else "/"
        tasks[task_name][str(ind)] = {
            "curr": 0,
            "total": 0,
            "eta": "-:-",
            "id": ind,
            "status": "Очередь",
            "name": cats.get(cat, urls),
            "city": main.cities[city],
        }
        tt = threading.Thread(target=func,
                              args=(task_name, cats.get(cat, urls), cat, city, wss_values, proxy, ind, None, pictures))
    pool.put(tt)
    return redirect(redirect_url)


@app.route("/add_check", methods=["POST"])
def add_check():
    city = request.form['city']
    form_file = request.files['urls']
    filename = form_file.filename.replace(".xlsx", f"_{time.time()}.xlsx")
    form_file.save(f"{filename}")
    form_file.close()
    df = pd.read_excel(f"{filename}").to_dict('records')

    proxy = request.form['proxy']
    f = open("prox", "w")
    f.write(proxy.replace("\r\n\r\n", "\n"))
    f.close()
    if request.form.get('ignore'):
        f = open("ignore", "w+")
        f.write(request.form['ignore'])
        f.close()
        ignore = [x.strip() for x in request.form['ignore'].split("\n")]
    else:
        f = open("ignore", "w+")
        f.close()
        ignore = []
    ind = str(int(time.time()))
    tasks['checker'][ind] = {"status": "В очереди"}
    tt = threading.Thread(target=main.checker,
                          args=("checker", df, proxy, city, ind, ignore, filename, wss_values))
    pool.put(tt)

    return redirect('/checker')


@app.route("/get_tasks")
def get_tasks():
    result = []
    for group in tasks.values():
        result.extend([int(x) for x in group.keys()])
    return result


@app.route("/add_checker")
def add_checker():
    if not request.cookies.get('login'):
        return redirect('/login')
    f = re.sub("\n+", "\n", open("prox").read())
    return render_template("add_check.html", prox=f, ignore=open("ignore").read())


threading.Thread(target=wss).start()
threading.Thread(target=ThreadPool).start()
app.config['UPLOAD_FOLDER'] = "."
app.run(host="0.0.0.0", port=81)
