import os
import time
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import json
from kafka import KafkaProducer

url: str = "https://divar.ir/s/iran/car"
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")
driver = webdriver.Chrome(
    options=options
)
driver.get(url)
time.sleep(15)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)


def send_to_kafka(sample_car):
    producer.send("divar_cars", value=sample_car)
    producer.flush()


def get_car_brand():
    try:
        car_brand = driver.find_element(
            By.XPATH,
            """//*[@id="app"]/div[1]/div/main/article/div/div[1]/section[1]/div[4]/div[1]/div[2]/span/a""",
        ).text
    except:
        car_brand = "Unknown"
    return car_brand


def get_car_title():
    try:
        car_title = driver.find_element(
            By.XPATH,
            """//*[@id="app"]/div[1]/div/main/article/div/div[1]/section[1]/div[1]/div/h1""",
        ).text
    except:
        car_title = "Unknown"
    return car_title


def get_car_city():
    try:
        car_city = driver.find_element(
            By.XPATH,
            """//*[@id="app"]/div[1]/div/main/article/div/div[1]/section[1]/button[1]/div[1]/div[1]/p""",
        ).text
    except:
        car_city = "Unknown"
    return car_city


def get_car_price():
    try:
        car_price = driver.find_element(
            By.XPATH,
            """//*[@id="app"]/div[1]/div/main/article/div/div[1]/section[1]/div[4]/div[5]/div[2]/p""",
        ).text
    except:
        car_price = "NaN"
    return car_price


def get_car_kilometers():
    try:
        car_kilometers = driver.find_element(
            By.XPATH,
            """//*[@id="app"]/div[1]/div/main/article/div/div[1]/section[1]/div[4]/table/tbody/tr/td[1]""",
        ).text
    except:
        car_kilometers = "NaN"
    return car_kilometers


def get_car_model():
    shamsi = "NaN"
    miladi = "NaN"
    try:
        car_model = driver.find_element(
            By.XPATH,
            """//*[@id="app"]/div[1]/div/main/article/div/div[1]/section[1]/div[4]/table/tbody/tr/td[2]""",
        ).text.split("-")
        for year in car_model:
            year = year.strip()
            if year.isdigit():
                year = int(year)
                if 1300 <= year <= 1500:
                    shamsi = year
                elif 1900 <= year <= 2100:
                    miladi = year
    except NoSuchElementException:
        pass
    return shamsi, miladi


def get_car_color():
    try:
        car_color = driver.find_element(
            By.XPATH,
            """//*[@id="app"]/div[1]/div/main/article/div/div[1]/section[1]/div[4]/table/tbody/tr/td[3]""",
        ).text
    except:
        car_color = "Unknown"
    return car_color


def get_car_gearbox():
    try:
        car_gearbox = driver.find_element(
            By.XPATH,
            """//*[@id="app"]/div[1]/div/main/article/div/div[1]/section[1]/div[4]/div[3]/div[2]/p""",
        ).text
    except:
        car_gearbox = "Unknown"
    return car_gearbox


def get_car_fuel():
    try:
        car_fuel = driver.find_element(
            By.XPATH,
            """//*[@id="app"]/div[1]/div/main/article/div/div[1]/section[1]/div[4]/div[4]/div[2]/p""",
        ).text
    except:
        car_fuel = "Unknown"
    return car_fuel


def get_car_image_src():
    img_src = []
    try:
        img_qty = int(
            driver.find_element(
                By.XPATH,
                """//*[@id="app"]/div[1]/div/main/article/div/div[2]/section[1]/div/div/div[1]/button[2]/span""",
            ).text
        )
        time.sleep(5)
        btn = driver.find_element(
            By.XPATH,
            """//*[@id="app"]/div[1]/div/main/article/div/div[2]/section[1]/div/div/div[1]/button[2]/i""",
        )
        btn.click()
        time.sleep(10)
        next_btn = driver.find_element(
            By.XPATH, """/html/body/div[6]/div/div[2]/div/button/i"""
        )
        time.sleep(5)
        for i in range(1, img_qty + 1):
            img = driver.find_element(
                By.XPATH,
                f"/html/body/div[6]/div/div[2]/div/div[{i}]/div[1]/figure/div/picture/img",
            )
            src = img.get_attribute("src")
            img_src.append(src)
            time.sleep(5)
            next_btn.click()
    except:
        pass
    return img_src


def get_car_details(car_url, car_id):
    driver.get(car_url)
    time.sleep(15)
    car_brand = get_car_brand()
    car_title = get_car_title()
    car_address = get_car_city()
    car_price = get_car_price()
    car_kilometers = get_car_kilometers()
    car_model_shamsi, car_model_miladi = get_car_model()
    car_color = get_car_color()
    car_gearbox = get_car_gearbox()
    car_fuel = get_car_fuel()
    car_images = get_car_image_src()
    sample_car = {
        "_id": car_id,
        "car_brand": car_brand,
        "car_title": car_title,
        "car_address": {
            "city": car_address.split("،")[0].split(" ")[-1],
            "address": car_address.split("،")[1:],
        },
        "car_price": car_price,
        "car_kilometers": car_kilometers,
        "car_details": {
            "car_model": {"shamsi": car_model_shamsi, "miladi": car_model_miladi},
            "car_color": car_color,
            "car_gearbox": car_gearbox,
            "car_fuel": car_fuel,
        },
        "car_images": car_images,
    }
    send_to_kafka(sample_car)
    print(sample_car)
    time.sleep(10)


seen_cars = set()
def get_car_link():
    cars_containers = driver.find_elements(
        By.CSS_SELECTOR, """div[class*="post-list__items-container"]"""
    )
    car_links = []
    for car_container in cars_containers:
        cars = car_container.find_elements(By.CSS_SELECTOR, 'a[href*="/v/"]')
        for car in cars:
            car_url = car.get_attribute("href")
            if not car_url:
                continue
            car_id = car_url.rstrip("/").split("/")[-1]
            if car_id in seen_cars:
                continue
            seen_cars.add(car_id)
            car_links.append((car_url, car_id))
    for car_url, car_id in car_links:
        get_car_details(car_url, car_id)


get_car_link()
# scroll_height_list = []
# def get_divar_cars():
#     while True:
#         try:
#             load_more_btn = driver.find_element(By.XPATH,'''//*[@id="post-list-container-id"]/div[2]/div/button''')
#             driver.execute_script("arguments[0].scrollIntoView();", load_more_btn)
#             time.sleep(10)
#             load_more_btn.click()
#             time.sleep(15)
#         except NoSuchElementException:
#             pass
#         get_car_link()
#         driver.execute_script("window.scrollBy(0,800)")
#         time.sleep(15)
#         new_height = driver.execute_script("return document.body.scrollHeight")
#         scroll_height_list.append(new_height)
#         if scroll_height_list.count(new_height) > 5:
#             scroll_height_list.clear()
#             time.sleep(120)
#             get_divar_cars()
# get_divar_cars()
