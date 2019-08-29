import sqlite3
import codecs

conn = sqlite3.connect('redis_aof.db')

c = conn.cursor()

def string_set_db(ll):
    key = ll[4]
    value = ll[6]
    try:
        c.execute('insert into STRING_KEY (KEY_NAME,VALUE) values (?,?);',(key,value))
        c.execute('insert into KEY_INFO (KEY_NAME,TYPE) values (?,?);',(key,'string'))
    except:
        pass

def hash_hmset_db(ll):
    list_len = int(ll[0][1:])
    key = ll[4]
    
    for i in range((list_len-2)//2):
        childkey = ll[4*i+6]
        value = ll[4*i+8]
        try:
            c.execute('insert into HASH_KEY (KEY_NAME,CHILD_KEY,VALUE) values (?,?,?);',(key,childkey,value))
        except:
            continue
    try:
        c.execute('insert into KEY_INFO (KEY_NAME,TYPE) values (?,?);',(key,'hash'))
    except:
        pass

    

def list_lpush_db(ll):
    list_len = int(ll[0][1:])
    key = ll[4]
    v_list=[]
    for i in range(list_len-2):
        value = ll[2*i+6]
        v_list.append(value)

    val2str = '||'.join(v_list[::-1])
    
    isexist=c.execute('select * from LIST_KEY where KEY_NAME = ?;',(key))
    if isexist==[]:
        c.execute('insert into LIST_KEY (KEY_NAME,VALUE) values (?,?);',(key,val2str))
        c.execute('insert into KEY_INFO (KEY_NAME,TYPE) values (?,?);',(key,'list'))
    else:
        val2str = val2str+'||'+isexist[0][1]
        c.execute('update LIST_KEY set VALUE = ? where KEY_NAME = ?;',(val2str,key))

    

def list_rpush_db(ll):
    list_len = int(ll[0][1:])
    key = ll[4]
    v_list=[]
    for i in range(list_len-2):
        value = ll[2*i+6]
        v_list.append(value)

    val2str = '||'.join(v_list)

    isexist=c.execute('select * from LIST_KEY where KEY_NAME = ?;',(key))
    if isexist==[]:
        c.execute('insert into LIST_KEY (KEY_NAME,VALUE) values (?,?);',(key,val2str))
        c.execute('insert into KEY_INFO (KEY_NAME,TYPE) values (?,?);',(key,'list'))
    else:
        val2str = isexist[0][1] +'||'+ val2str
        c.execute('update LIST_KEY set VALUE = ? where KEY_NAME = ?;',(val2str,key))



def set_sadd_db(ll):
    list_len = int(ll[0][1:])
    key = ll[4]
    try:
        for i in range(list_len-2):
            value = ll[2*i+6]
            
            c.execute('insert into SET_KEY (KEY_NAME,VALUE) values (?,?);',(key,value))
        c.execute('insert into KEY_INFO (KEY_NAME,TYPE) values (?,?);',(key,'set'))

    except:
        return 

def zset_zadd_db(ll):
    list_len = int(ll[0][1:])
    key = ll[4]
    try:
        for i in range((list_len-2)//2):
            score = float(ll[4*i+6])
            value = ll[4*i+8]
            c.execute('insert into ZSET_KEY (KEY_NAME,SCORE,VALUE) values (?,?,?);',(key,score,value))
        c.execute('insert into KEY_INFO (KEY_NAME,TYPE) values (?,?);',(key,'zset'))

    except:
        return 

def process(s):
    order_list = s.split('\r\n')
    # print(order_list)
    op = order_list[2]
    # if op == 'SET':
    #     string_set_db(order_list)

    # elif op == 'HMSET':
    #     hash_hmset_db(order_list)

    # elif op == 'LPUSH' :
    #     list_lpush_db(order_list)

    # elif op == 'RPUSH':
    #     list_rpush_db(order_list)

    # elif op == 'SADD':
    #     set_sadd_db(order_list)
    
    if op == 'ZADD':
        zset_zadd_db(order_list)

with codecs.open('appendonly.aof','r','utf-8') as f:
    s=''
    for line in f:
        if line.startswith('*') and s!='':
            process(s)
            s=line
        else:
            s+=line
    if s!='':
        process(s)

conn.commit()
conn.close()