FROM astrocrpublic.azurecr.io/runtime:3.0-10

COPY requirements.txt . 
RUN pip install -r requirements.txt