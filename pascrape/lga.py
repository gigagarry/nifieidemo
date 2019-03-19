
from bs4 import BeautifulSoup
from pyvirtualdisplay import Display
from selenium import webdriver
import json
import datetime


data = {}
url = 'https://www.laguardiaairport.com/'
try:
    display = Display(visible=0, size=(800, 600))
    display.start()
    browser = webdriver.PhantomJS()
    browser.get(url)
    innerHTML = browser.execute_script("return document.body.innerHTML") #returns the inner HTML as a stringbr
    browser.quit()
    soup = BeautifulSoup('<!doctype html>'+innerHTML, 'html.parser')
    weightMatrix = []
    links = soup.find_all('div', 'terminals-lot')
    for link in links:
        data = {}
        desc = link.parent.text.split("\n")[0]
        pct = link.parent.next_sibling.next_sibling.next.next.next.text.split()[0].split("%")[0]
        data["lot"]=desc.strip()
        data["pct"]=int(pct)
        data['airport']="LGA"
        data['asof']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        weightMatrix.append(data)
    json_data = json.dumps(weightMatrix)
    print(json_data)
except Exception as e:
    browser.quit()