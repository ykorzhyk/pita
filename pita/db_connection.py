from sqlalchemy import create_engine, MetaData

metadata = MetaData()

engine = create_engine('postgresql://test:test@postgres:5432/pita', echo=True)

connection = engine.connect()
