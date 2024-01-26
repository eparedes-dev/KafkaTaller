from flask import Flask, request
from confluent_kafka import Producer
from confluent_kafka import KafkaException
import socket
import json
import threading
from flask_sqlalchemy import SQLAlchemy
import schedule
import time

app = Flask(__name__)

conf = {
    'bootstrap.servers': 'pkc-4r087.us-west2.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'DDUTHVQCYDVVQOXM',
    'sasl.password': 'sexksNS0i9bwEDEpW3mEGEAzTIov7FCQimL7EMR1riY56EaNL/0X9lntltrP5V+c',
    'client.id': socket.gethostname()
}

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:root@localhost/tallerIntegracionKafka'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
producer = Producer(conf)

class Mensajes(db.Model):
    EnvioID = db.Column(db.Integer, primary_key=True)
    Contenido = db.Column(db.String(255))
    Topic = db.Column(db.String(50))
    Recibido = db.Column(db.Boolean, default=False)

@app.route('/nacional', methods=['POST'])
def nacional():
    try:
        data = request.get_json()
        data = json.dumps(data).encode('utf-8')
        producer.produce('Nacional', value=data)

        mensaje = Mensajes(Contenido=data, Topic='Nacional', Recibido=True)
        db.session.add(mensaje)
        db.session.commit()
        return 'Mensaje enviado al Topic Nacional y guardado en la base de datos', 200
    except Exception as e:
        # Capturar excepciones generales
        db.session.rollback()
        mensaje = Mensajes(Contenido=data, Topic='Nacional', Recibido=False)
        db.session.add(mensaje)
        db.session.commit()
        print(f'Error al enviar el mensaje: {type(e).__name__} - {str(e)}')
        return f'Error general al enviar el mensaje: {str(e)}', 500

@app.route('/internacional', methods=['POST'])
def internacional():
    try:
        data = request.get_json()
        data = json.dumps(data).encode('utf-8')
        producer.produce('Internacional', value=data)
        mensaje = Mensajes(Contenido=data, Topic='Internacional', Recibido=True)
        db.session.add(mensaje)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        mensaje = Mensajes(Contenido=data, Topic='Internacional', Recibido=False)
        db.session.add(mensaje)
        db.session.commit()
        return f'Error al enviar el mensaje: {str(e)}', 500
    return 'Mensaje enviado al Topic Internacional y guardado en la base de datos', 200

def retry_unsent_messages():
    with app.app_context():
        unsent_messages = get_unsent_messages()
        for message in unsent_messages:
            try:
                producer.produce(message.Topic, value=message.Contenido)
                mark_as_sent(message)
                print(f"Message sent to {message.Topic} topic and updated in the database")
            except Exception as e:
                print(f"Error sending the message: {str(e)}")

schedule.every(60).seconds.do(retry_unsent_messages)

def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(1)

def get_unsent_messages():
    mensajes =  Mensajes.query.filter_by(Recibido=False).all()
    return Mensajes.query.filter_by(Recibido=False).all()

def mark_as_sent(message):
    message.Recibido = True
    db.session.commit()

if __name__ == '__main__':
    threading.Thread(target=run_schedule, daemon=True).start()
    app.run(debug=True)

