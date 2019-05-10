import sys, os
import pypyodbc
import time

CONNECTION_STRING = 'Driver={SQL Server Native Client 11.0};;Server=(localdb)\ProjectsV13;Database=SD2E-Protein-Design;Trusted_Connection=yes'

def is_float(n):
    try:
        float(n)
    except ValueError:
        return False
    return True


def index_exists(dataset, name):
    conn = pypyodbc.connect(CONNECTION_STRING)
    cur = conn.cursor()

    try:
        sql = "select COUNT(1) from index_key where name like ? and dataset like ?;"
        cur.execute(sql, (name, dataset))
        n = cur.fetchall()
    except pypyodbc.ProgrammingError as e:
        print (e)
    
    return n[0][0] > 0

def row_exists(table, dataset, name):
    conn = pypyodbc.connect(CONNECTION_STRING)
    cur = conn.cursor()

    try:
        sql = "select COUNT(1) from {0} where id = (SELECT id from index_key WHERE dataset like ? and name like ?);".format(table)
        cur.execute(sql, (dataset, name))
        n = cur.fetchall()
    except pypyodbc.ProgrammingError as e:
        print (e)
    
    return n[0][0] > 0

def push_index(dataset, name):
    conn = pypyodbc.connect(CONNECTION_STRING)
    cur = conn.cursor()
    print("adding " + name)
    try:
        cur.execute('INSERT INTO index_key (dataset, name, timestamp) VALUES (?, ?, ?);', (dataset, name, time.strftime('%Y-%m-%d %H:%M:%S')))
        cur.commit()
    except pypyodbc.ProgrammingError as e:
        print (e)

def pushRow(table, keys, vals, dataset, name):
    if not row_exists(table, dataset, name):
        newvals = []
        for v in vals:
            if is_float(v):
                newvals.append(v)
            else:
                newvals.append("'"+v+"'")
        if not index_exists(dataset, name):
            push_index(dataset, name)
        conn = pypyodbc.connect(CONNECTION_STRING)
        cur = conn.cursor()

        try:
            sql = 'INSERT INTO {0} (id, {1}, timestamp) VALUES ((SELECT id from index_key WHERE dataset like ? and name like ?), {2}, {3});'.format(table,', '.join(keys), ', '.join(newvals), "'"+time.strftime('%Y-%m-%d %H:%M:%S')+"'")
            cur.execute(sql, (dataset, name))
            cur.commit()
            print ("Entry {0}:{1} added to table {2}\n".format(dataset,name,table))
        except (pypyodbc.ProgrammingError, pypyodbc.DataError) as e:
            print (dataset, name)
            print (e)


def trim_db(csv):
    i = 0
    for r in csv.split('\n'):
        if 'dataset' in r and 'name' in r:
            break
        i+=1
    return '\n'.join(csv.split('\n')[i:])

def main():
    df_path = sys.argv[1]
    dataframe = trim_db(open(df_path, 'r').read())
    table = os.path.split(df_path)[1].split('.')[-2] # not .csv
    rows = dataframe.split('\n')
    keys = rows[0].split(',')[2:]
    for r in rows[1:]:
        if len(r.split(',')) > 1:
            pushRow(table, keys, r.split(',')[2:], r.split(',')[0], r.split(',')[1])
    return


if __name__ == '__main__':
    main()

