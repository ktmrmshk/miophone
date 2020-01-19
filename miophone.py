# -*- coding: utf-8 -*-

import pandas as pd
import os,sys
from datetime import time, datetime
import miophone_config
import mysql.connector
import json



class miophone(object):
    def __init__(self):
        self.df = pd.DataFrame()
        self.conf = miophone_config.mio_conf

    def addFractDigit(self, s):
        '''
        in, out
        12.45 => 12.450000
        1.0 =>    1.000000
        12  => 12
        '''
        cnt=0
        in_fract = False
        for i in range(len(s)):
            if s[i] == '.':
                in_fract = True
            elif in_fract:
                cnt+=1
        if cnt > 0 and cnt < 6:
            s+='0'*(6-cnt)
        return s

    def scanMioService(self, df_orig):
        '''
        out: d_time, dur, miophone, familycall
        '''
        d_time=list()
        dur=list()
        miophone=list()
        familycall=list()
        for i, r in df_orig.iterrows():
            # d_time
            s='{} {}'.format(r['通話年月日'], r['通話開始時刻'])
            t=datetime.strptime(s, '%Y%m%d %H:%M:%S')
            d_time.append(t)

            # dur
            d=time.fromisoformat(self.addFractDigit( r['通話時間'] ))
            d_sec = 3600*d.hour + 60 * d.minute + d.second + d.microsecond *1e-6
            dur.append(d_sec)

            # miophone
            if pd.isnull( r['通話の種類'] ):
                miophone.append(False)
            else:
                miophone.append(True)

            # family call
            if r['ファミリー通話割引'] == '-':
                familycall.append(False)
            else:
                familycall.append(True)


        return d_time, dur, miophone, familycall


    def read_mio_csv(self, path):
        # add ending ',' to first line
        buf=None
        with open(path, 'r', encoding='shift_jis') as f:
            buf = f.read()

        lines = buf.split('\n')
        lines[0]+=','
        new_buf = '\n'.join(lines)
        tmp_file = path+'.tmp'
        with open(tmp_file, 'w', encoding='utf-8') as f:
            f.write(new_buf)

        # read from csv
        df_orig = pd.read_csv(tmp_file, dtype = {'お客様の電話番号':'object', '通話先電話番号':'object'})

        # rm tmp_file
        os.remove(tmp_file)

        ### set new data
        self.df['src'] = df_orig['お客様の電話番号']
        self.df['dest'] = df_orig['通話先電話番号']
        self.df['toll'] = df_orig['料金']
        d_time, dur, miophone, familycall = self.scanMioService(df_orig)
        self.df['d_time'] = d_time
        self.df['dur'] = dur
        self.df['miophone'] = miophone
        self.df['familycall'] = familycall

    def connect_db(self):
        self.conn = mysql.connector.connect(user=self.conf['mysql_user'], password=self.conf['mysql_passwd'], host=self.conf['mysql_host'], port=self.conf['mysql_port'], auth_plugin='mysql_native_password')
        self.cur=self.conn.cursor()

    def make_scheme(self):
        scheme_json = '''
        {
            "src": "varchar(20)",
            "dest": "varchar(20)",
            "toll": "int",
            "d_time": "datetime",
            "dur": "float",
            "miophone": "bool",
            "familycall": "bool"
        }
        '''
        self.scheme = json.loads(scheme_json)

    def create_db(self):
        self.connect_db()

        self.cur.execute('create database {};'.format(self.conf['dbname']))
        
        self.make_scheme()
        sch_list = list() 
        for k,v in self.scheme.items():
            sch_list.append('{} {}'.format(k, v))
        sch = ','.join(sch_list)

        self.cur.execute('create table {}.{} ({});'.format(self.conf['dbname'], self.conf['dbtable'], sch))

        self.conn.commit()

    def ingest_df(self):
        self.connect_db()
        self.make_scheme()
        labels=list(self.scheme.keys())
        print(labels)
        for i, k in self.df.iterrows():
            values=list()
            for l in labels:
                if ' ' in str(k[l]) or str( k[l] ).startswith('0'):
                    values.append( "'{}'".format( str(k[l]) ))
                else:
                    values.append( str(k[l]))
            sql = 'insert into {}.{} ({}) values ({});'.format(self.conf['dbname'], self.conf['dbtable'], ','.join(labels), ','.join(values))
            print(sql)
            self.cur.execute(sql)

        self.conn.commit()    



def do_test():
    mio=miophone()
    mio.read_mio_csv('call_log.csv')
    print(mio.df.head())
    
    try:
        mio.create_db()
    except mysql.connector.errors.DatabaseError as e:
        print(e)
    
    mio.ingest_df()

def app(argv):
    if len(argv) != 2:
        print('option err')
        print('usage: python3 miophone.py filename')
        print('ex   : python3 miophone.py call_log.csv')
        exit()
    
    print('filename: {}'.format(argv[1]))

    mio=miophone()
    mio.read_mio_csv(argv[1])
    print(mio.df.head())
    
    try:
        mio.create_db()
    except mysql.connector.errors.DatabaseError as e:
        print(e)
    
    mio.ingest_df()

   

if __name__ == '__main__':
    #do_test()
    app(sys.argv)

