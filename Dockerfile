FROM continuumio/anaconda3:2023.03-1

WORKDIR /app
COPY . .
EXPOSE 7860
RUN chmod -R 777 /app
RUN pip install -r requirements.txt

#RUN sed -i 's/http/https/g' /etc/apt/sources.list
#RUN apt install tesseract-ocr -y
#RUN apt install libtesseract-dev -y

CMD ["python","app.py"]