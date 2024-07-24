from datetime import datetime
import smtplib
from datetime import datetime
today=(datetime.now().month,(datetime.now().day))
import pandas as pd
# pd.read_csv("birthday.csv")
data=pd.read_csv("birthday.csv")
dicbirthday={}
for index,row in data.iterrows():
  dicbirthday[(row["month"],row["day"])]=row
if today in dicbirthday:
    # print(dicbirthday[today])
   person=dicbirthday[today]
   filePath="letter.txt"
   my_email="icodeguru007@gmail.com"
   password="*****"
   with open(filePath) as letter:
     letter=letter.read()
     letter=letter.replace("[Name]",person["Name"])
   with smtplib.SMTP("smtp.gmail.com:587") as connection:
        connection.starttls()
        connection.login(my_email,password)
        connection.sendmail(from_addr=my_email,
        to_addrs=my_email , 
        msg=f"Subject: Happy Birthday\n\n{letter}".encode('utf-8'))
