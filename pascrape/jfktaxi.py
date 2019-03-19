
from bs4 import BeautifulSoup
from pyvirtualdisplay import Display
from selenium import webdriver
import json
import datetime


data = {}
url = 'https://www.jfkairport.com/'
try:
    display = Display(visible=0, size=(800, 600))
    display.start()
    browser = webdriver.PhantomJS()
    browser.get(url)
    innerHTML = browser.execute_script("return document.body.innerHTML") #returns the inner HTML as a stringbr
    browser.quit()
    soup = BeautifulSoup('<!doctype html>'+innerHTML, 'html.parser')
    weightMatrix = []

    links = soup.find_all('div', 'term-desc')
    for link in links:
        if len(link['class']) == 1 and not(link.find('small') is None):
            data = {}
            desc = link.next.next.next_sibling.next_sibling.next.next.split("\n")[0]
            wait = link.find('span').next
            units = link.find('small').next
            data["terminal"]=desc.strip()
            data["wait"]=int(wait)
            data["units"] = units
            data['airport']="JFK"
            data['asof']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            weightMatrix.append(data)

    json_data = json.dumps(weightMatrix)
    print(json_data)
except Exception as e:
    browser.quit()
