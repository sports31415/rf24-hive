from sqlalchemy.engine import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import sessionmaker
import json


def set_connection():
    Base = declarative_base()

    with open('../credentials.json', 'r') as data_file:
        cred = json.load(data_file)

    engine = create_engine('mysql+mysqldb://root:' + cred['password'] + '@192.168.0.9/hive?charset=utf8', echo=True)
    metadata = MetaData(bind=engine)

    class temperature(Base):
            __table__ = Table('temperature', metadata, autoload = True)

    #set up a session
    Session = sessionmaker(bind=engine)
    session = Session()

    return session, temperature


def insert_temperature(session, table_class, sensor_id, value, load_datetime):
    new_entry = table_class(sensor_id=sensor_id, temperature=value, load_datetime=load_datetime)
    session.add(new_entry)

def db_commit(session):
    try:
        session.commit()
    except:
        session.rollback()

