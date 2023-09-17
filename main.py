from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, update, select, delete

metadata = MetaData()

stations_table = Table(
    'stations',
    metadata,
    Column('station', String, primary_key=True),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String)
)

measure_table = Table(
    'measure',
    metadata,
    Column('station', String),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Integer)
)


def import_data(conn, filecsv, destination):
    with open(filecsv, "r") as file:
        next(file)
        for line in file:
            line = line.replace("\n", "")
            line = line.replace("\r", "")
            data = tuple(line.split(","))
            conn.execute(destination.insert().values(data))


if __name__ == "__main__":
    engine = create_engine('sqlite:///mydatabase.db')
    conn = engine.connect()
    metadata.create_all(engine)

    stations_data = import_data(conn, "clean_stations.csv", stations_table)
    measure_data = import_data(conn, "clean_measure.csv", measure_table)

    result = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
    for row in result:
        print(row)
    
    update_stmt = stations_table.update().where(stations_table.c.elevation == 3).values(name="Waikiki")
    conn.execute(update_stmt)

    select_update = select([stations_table]).where(stations_table.c.elevation == 3)
    update_result = conn.execute(select_update)
    for row in update_result:
        print(row)

    delete_stmt = delete(stations_table).where(stations_table.c.elevation > 7)
    conn.execute(delete_stmt)
    
    select_stmt = select([stations_table])
    delete_result = conn.execute(select_stmt)
    for row in delete_result:
        print(row)

    conn.close()