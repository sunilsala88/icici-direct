import credentials as cs
acc_pass = cs.acc_pass
totp_key = cs.totp_key
acc_id = cs.acc_id
api_key = cs.api_key
secret = cs.secret_key

#3.12.8
#1.0.62
#2nd apr 2025

# Import necessary libraries
from selenium import webdriver
from selenium.webdriver.common.by import By
import pyotp
import time
from selenium.webdriver.chrome.options import Options


# Import BreezeConnect from breeze_connect module
from breeze_connect import BreezeConnect

# Initialize SDK
breeze = BreezeConnect(api_key=api_key)

# Obtain the session key from the API endpoint
import urllib
url = "https://api.icicidirect.com/apiuser/login?api_key=" + urllib.parse.quote_plus(api_key)
print(url)



# # Set Chrome options for headless mode
# options = Options()
# options.add_argument('--headless=new')

# Initialize Chrome webdriver
driver = webdriver.Chrome()

# Open the URL in the Chrome browser
driver.get(url)
time.sleep(1)
# Enter account ID, password, and accept terms and conditions
driver.find_element(By.XPATH, '//*[@id="txtuid"]').send_keys(acc_id)
driver.find_element(By.XPATH, '//*[@id="txtPass"]').send_keys(acc_pass)
driver.find_element(By.XPATH, '//*[@id="chkssTnc"]').click()
time.sleep(1)
# Submit the login form
driver.find_element(By.XPATH, '//*[@id="btnSubmit"]').click()

# Wait for the OTP input fields to appear
time.sleep(3)

# Generate TOTP code
totp = pyotp.TOTP(totp_key)
t = totp.now()

# Enter the TOTP code in the OTP input fields
driver.find_element(By.XPATH, '//*[@id="pnlOTP"]/div[2]/div[2]/div[3]/div/div[1]/input').send_keys(t[0])
driver.find_element(By.XPATH, '//*[@id="pnlOTP"]/div[2]/div[2]/div[3]/div/div[2]/input').send_keys(t[1])
driver.find_element(By.XPATH, '//*[@id="pnlOTP"]/div[2]/div[2]/div[3]/div/div[3]/input').send_keys(t[2])
driver.find_element(By.XPATH, '//*[@id="pnlOTP"]/div[2]/div[2]/div[3]/div/div[4]/input').send_keys(t[3])
driver.find_element(By.XPATH, '//*[@id="pnlOTP"]/div[2]/div[2]/div[3]/div/div[5]/input').send_keys(t[4])
driver.find_element(By.XPATH, '//*[@id="pnlOTP"]/div[2]/div[2]/div[3]/div/div[6]/input').send_keys(t[5])

# Wait for the OTP verification to complete
time.sleep(2)

# Click the submit button to complete the login process
driver.find_element(By.XPATH, '//*[@id="Button1"]').click()

# Wait for the page to load
time.sleep(2)

# Get the new URL after login
newurl = driver.current_url
print(newurl)

# Extract the authorization code from the URL
auth_code = newurl.split("=")[1]

# Quit the Chrome browser
driver.quit()

# Write the authorization code to a file
a = open("access.txt", 'w')
a.write(auth_code)
a.close()