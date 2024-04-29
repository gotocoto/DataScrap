from selenium import webdriver 
from selenium.webdriver.common.by import By 

# Create a new instance of the Chrome driver 
option = webdriver.ChromeOptions() 

option.add_argument("--disable-gpu") 
option.add_argument("--disable-extensions") 
option.add_argument("--disable-infobars") 
option.add_argument("--start-maximized") 
option.add_argument("--disable-notifications") 
option.add_argument('--headless') 
option.add_argument('--no-sandbox') 
option.add_argument('--disable-dev-shm-usage') 
driver = webdriver.Chrome(options=option) 

# Navigate to the news website 
driver.get("https://www.nytimes.com/") 

# Scrape the headlines 
headlines = driver.find_elements(By.CLASS_NAME, "indicate-hover") 
print("i am running") 
for headline in headlines: 
	print(headline.text) 
print("i am closed") 
# Close the browser 
driver.quit() 
