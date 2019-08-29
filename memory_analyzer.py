import sqlite3
import codecs
import bisect
import random

ZSKIPLIST_MAXLEVEL=32
ZSKIPLIST_P=0.25
POINTER_SIZE = 8
LONG_SIZE = 8 
REDIS_SHARED_INTEGERS = 10000
hash_max_ziplist_entries = 512
hash_max_ziplist_value = 64
zset_max_ziplist_value =64
zset_max_ziplist_entries =128

def hashtable_entry_overhead():
    return 2*POINTER_SIZE+8

def hashtable_overhead(size):
    return 4+POINTER_SIZE*4+7*LONG_SIZE+POINTER_SIZE*next_power(size)*1.5
    #return 4+POINTER_SIZE*4+7*LONG_SIZE+0.5*POINTER_SIZE*next_power(size)
def malloc_overhead(l):
    alloc = get_jemalloc_allocation(l)
    return alloc

def robj_overhead():
    return POINTER_SIZE+8

def top_level_object_overhead(key):
    return hashtable_entry_overhead()+sizeof_string(key)+robj_overhead()

def ziplist_entry_overhead(value):
        # See https://github.com/antirez/redis/blob/unstable/src/ziplist.c
    if value.isdigit():
        header = 1
        value=int(value)
        if value < 12:
            size = 0
        elif value < 2**8:
            size = 1
        elif value < 2**16:
            size = 2
        elif value < 2**24:
            size = 3
        elif value < 2**32:
            size = 4
        else:
            size = 8
    else:
        size = len(value)
        if size <= 63:
            header = 1
        elif size <= 16383:
            header = 2
        else:
            header = 5
    # add len again for prev_len of the next record 
    prev_len = 1 if size < 254 else 5

    return prev_len + header + size

def ziplist_header_overhead():
    return 4+4+2+1

def quicklist_overhead(cur_zips):
    quicklist = 2*POINTER_SIZE+LONG_SIZE+2*4
    quickitem = 4*POINTER_SIZE+LONG_SIZE+2*4
    return quicklist + cur_zips*quickitem

def skiplist_overhead(size):
    return 2*POINTER_SIZE+hashtable_overhead(size)+(2*POINTER_SIZE+16)

def skiplist_entry_overhead():
    return hashtable_entry_overhead()+2*POINTER_SIZE+8+(POINTER_SIZE+8)*zset_random_level()

def sizeof_string(ss):
    try:
        num = int(ss)
        if num<REDIS_SHARED_INTEGERS:
            return 0
        else:
            return 0
    except ValueError:
        pass
    l = len(ss)
    if l<2**5:
        return malloc_overhead(l+1+1)
    elif l<2**8:
        return malloc_overhead(l+1+2+1)
    elif l<2**16:
        return malloc_overhead(l+1+4+1)
    elif l<2**32:
        return malloc_overhead(l+1+8+1)
    return malloc_overhead(l+1+16+1)


def next_power(size):
    power = 1
    while (power <= size) :
        power = power << 1
    return power


def is_integer_type(element):
    if isinstance(element,int):
        return True
    return False

def element_length(element):
    if is_integer_type(element):
        return LONG_SIZE
    return len(element)

def zset_random_level():
    level = 1
    rint = random.randint(0, 0xFFFF)
    while (rint < ZSKIPLIST_P * 0xFFFF):
        level += 1
        rint = random.randint(0, 0xFFFF)        
    if level < ZSKIPLIST_MAXLEVEL :
        return level
    else:
        return ZSKIPLIST_MAXLEVEL

def is_number(s):
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
 
    return False

jemalloc_size_classes = [
    8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, 448, 512, 640, 768, 896, 1024,
    1280, 1536, 1792, 2048, 2560, 3072, 3584, 4096, 5120, 6144, 7168, 8192, 10240, 12288, 14336, 16384, 20480, 24576,
    28672, 32768, 40960, 49152, 57344, 65536, 81920, 98304, 114688,131072, 163840, 196608, 229376, 262144, 327680,
    393216, 458752, 524288, 655360, 786432, 917504, 1048576, 1310720, 1572864, 1835008, 2097152, 2621440, 3145728,
    3670016, 4194304, 5242880, 6291456, 7340032, 8388608, 10485760, 12582912, 14680064, 16777216, 20971520, 25165824,
    29360128, 33554432, 41943040, 50331648, 58720256, 67108864, 83886080, 100663296, 117440512, 134217728, 167772160,
    201326592, 234881024, 268435456, 335544320, 402653184, 469762048, 536870912, 671088640, 805306368, 939524096,
    1073741824, 1342177280, 1610612736, 1879048192, 2147483648, 2684354560, 3221225472, 3758096384, 4294967296,
    5368709120, 6442450944, 7516192768, 8589934592, 10737418240, 12884901888, 15032385536, 17179869184, 21474836480,
    25769803776, 30064771072, 34359738368, 42949672960, 51539607552, 60129542144, 68719476736, 85899345920,
    103079215104, 120259084288, 137438953472, 171798691840, 206158430208, 240518168576, 274877906944, 343597383680,
    412316860416, 481036337152, 549755813888, 687194767360, 824633720832, 962072674304, 1099511627776,1374389534720,
    1649267441664, 1924145348608, 2199023255552, 2748779069440, 3298534883328, 3848290697216, 4398046511104,
    5497558138880, 6597069766656, 7696581394432, 8796093022208, 10995116277760, 13194139533312, 15393162788864,
    17592186044416, 21990232555520, 26388279066624, 30786325577728, 35184372088832, 43980465111040, 52776558133248,
    61572651155456, 70368744177664, 87960930222080, 105553116266496, 123145302310912, 140737488355328, 175921860444160,
    211106232532992, 246290604621824, 281474976710656, 351843720888320, 422212465065984, 492581209243648,
    562949953421312, 703687441776640, 844424930131968, 985162418487296, 1125899906842624, 1407374883553280,
    1688849860263936, 1970324836974592, 2251799813685248, 2814749767106560, 3377699720527872, 3940649673949184,
    4503599627370496, 5629499534213120, 6755399441055744, 7881299347898368, 9007199254740992, 11258999068426240,
    13510798882111488, 15762598695796736, 18014398509481984, 22517998136852480, 27021597764222976,31525197391593472,
    36028797018963968, 45035996273704960, 54043195528445952, 63050394783186944, 72057594037927936, 90071992547409920,
    108086391056891904, 126100789566373888, 144115188075855872, 180143985094819840, 216172782113783808,
    252201579132747776, 288230376151711744, 360287970189639680, 432345564227567616, 504403158265495552,
    576460752303423488, 720575940379279360, 864691128455135232, 1008806316530991104, 1152921504606846976,
    1441151880758558720, 1729382256910270464, 2017612633061982208, 2305843009213693952, 2882303761517117440,
    3458764513820540928, 4035225266123964416, 4611686018427387904, 5764607523034234880, 6917529027641081856,
    8070450532247928832, 9223372036854775808, 11529215046068469760, 13835058055282163712, 16140901064495857664
]

def get_jemalloc_allocation(size):
    idx = bisect.bisect_left(jemalloc_size_classes, size)
    alloc = jemalloc_size_classes[idx] if idx < len(jemalloc_size_classes) else size
    return alloc

def str_cost(key,value):
    size=sizeof_string(value)+top_level_object_overhead(key)
    length = element_length(value)
    return [int(size),length]

def hash_cost(key ,child_key,value):
    zipvalue_len_list = []
    sdsvalue_len_list = []
    list_len = len(child_key)
    for i in range(list_len):
        
        zipvalue_len_list.append(ziplist_entry_overhead(child_key[i]))
        zipvalue_len_list.append(ziplist_entry_overhead(value[i]))
        sdsvalue_len_list.append(sizeof_string(child_key[i]))
        sdsvalue_len_list.append(sizeof_string(value[i]))
        
    if max(sdsvalue_len_list) <=hash_max_ziplist_value and list_len <= hash_max_ziplist_entries:
        return (int(top_level_object_overhead(key)+ziplist_header_overhead()+sum(zipvalue_len_list)),list_len,'ziplist')
    else:
        size = top_level_object_overhead(key)+ hashtable_overhead(list_len) +(hashtable_entry_overhead()+2*robj_overhead())*(list_len)+sum(sdsvalue_len_list)
        return [int(size),list_len,'hash']

def list_cost(key,value):
    size = top_level_object_overhead(key)
    list_max_ziplist_size = 8192
    list_compress_depth = 0
    cur_zips = 1
    cur_zip_size = 0
    list_len = len(value)
    for i in range(list_len):
        
        size_in_zip = ziplist_entry_overhead(value[i])
        if cur_zip_size+size_in_zip >list_max_ziplist_size:
            cur_zip_size = size_in_zip
            cur_zips +=1
        else:
            cur_zip_size += size_in_zip
        list_item_zipped_size += ziplist_entry_overhead(value[i])
    
    size+=quicklist_overhead(cur_zips)
    size+=ziplist_header_overhead()*cur_zips
    size+=list_item_zipped_size

    return [int(size),list_len,'quicklist']

def set_cost(key,value):
    size = top_level_object_overhead(key)
    li_num = len(value)
    size+= hashtable_overhead(li_num)

    for i in range(li_num):
        size+=sizeof_string(value[i])
        size+=hashtable_entry_overhead()
        size+=robj_overhead()
    
    return [int(size),li_num,'hash']

def zset_cost(key,point,value):
    zipvalue_len_list = []
    sdsvalue_len_list = []
    size = top_level_object_overhead(key) #24
    li_num = len(point)
    size+=skiplist_overhead(li_num) 
    for i in range(li_num):
        if point[i][-2:]=='.0':
            zipvalue_len_list.append(ziplist_entry_overhead(point[i][:-2]))
        else:
            zipvalue_len_list.append(ziplist_entry_overhead(point[i]))
        zipvalue_len_list.append(ziplist_entry_overhead(value[i]))
        sdsvalue_len_list.append(sizeof_string(point[i]))
        sdsvalue_len_list.append(sizeof_string(value[i]))
        size+=8
        size+=sizeof_string(value[i])
        size+=robj_overhead()
        size+=skiplist_entry_overhead()
    if max(sdsvalue_len_list) <= zset_max_ziplist_value and li_num <= zset_max_ziplist_entries:
        return [int(top_level_object_overhead(key)+ziplist_header_overhead()+sum(zipvalue_len_list)),li_num,'ziplist']
    else:
        return [int(size),li_num,'skiplist']

conn = sqlite3.connect('redis_aof.db')

c = conn.cursor()

key_info=c.execute("select * from KEY_INFO")
key_info = list(key_info)
for info in key_info:
    key_type = info[1]
    key_name = info[0]
    if key_type == 'string':

        str_info = c.execute("select * from STRING_KEY where KEY_NAME =?",(key_name,))
        str_info=str_info.fetchall()    
        value = str_info[0][1]
        size,llen = str_cost(key_name,value)
        c.execute("update KEY_INFO set ENCODING=? ,BYTE=? ,LENGTH=? where KEY_NAME=?",('sds',size,llen,key_name))
    
    if key_type == 'hash':

        hash_info = c.execute("select * from HASH_KEY where KEY_NAME =?",(key_name,))
        child_key = []
        value = []

        for x in hash_info:
            child_key.append(x[1])
            value.append(x[2])
        size,llen,encode_way= hash_cost(key_name,child_key,value)
        c.execute("update KEY_INFO set ENCODING=? ,BYTE=? ,LENGTH=? where KEY_NAME=?",(encode_way,size,llen,key_name))

    if key_type =='list':
        list_info = c.execute("select * from LIST_KEY where KEY_NAME =?",(key_name,))
        list_info = list_info.fetchall()
        value = list_info[0][1]
        value = value.split('||')
        size,llen,encode_way= list_cost(key_name,value)
        c.execute("update KEY_INFO set ENCODING=?,BYTE=?,LENGTH=? where KEY_NAME=?",(encode_way,size,llen,key_name))

    if key_type =='set':
        set_info = c.execute("select * from SET_KEY where KEY_NAME =?",(key_name,))
        value = [x[1] for x in set_info]
        size,llen,encode_way= set_cost(key_name,value)
        c.execute("update KEY_INFO set ENCODING=?,BYTE=?,LENGTH=? where KEY_NAME=?",(encode_way,size,llen,key_name))

    if key_type == 'zset':
        zset_info = c.execute("select * from ZSET_KEY where KEY_NAME =?",(key_name,))
        score = []
        value = []
        for x in zset_info:

            score.append(str(x[2]))
            value.append(x[1])
        size,llen,encode_way= zset_cost(key_name,score,value)
        c.execute("update KEY_INFO set ENCODING=?,BYTE=?,LENGTH=? where KEY_NAME=?",(encode_way,size,llen,key_name))

conn.commit()
conn.close()