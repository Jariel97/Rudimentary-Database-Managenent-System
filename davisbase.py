import os
import struct
import sys
import operator
import re
import pdb
from datetime import datetime, time
import sqlparse


SIZE_OF_PAGE = 512
BYTE_ORDER = sys.byteorder
if BYTE_ORDER=='big':
    endian = '>'
elif BYTE_ORDER=='little':
    endian = '<'

def read_input(cmd_input):
    if len(cmd_input)==0:
        pass
    elif cmd_input[-1]!=";":
        return cmd_input
    elif cmd_input.lower() == "help;":
        print_help()
    elif cmd_input.lower() == "show tables;":
        show_tabs()
    elif cmd_input[0:len("drop table ")].lower() == "drop table ":
        drop_tab(cmd_input)
    elif cmd_input[0:len("insert ")].lower() == "insert ":
        insert_into(cmd_input)
    elif cmd_input[0:len("create table ")].lower() == "create table ":
        create_tab(cmd_input)
    elif cmd_input[0:len("select ")].lower() == "select ":
        tab_name, cells = where(cmd_input)
        if tab_name==None:
            return
        tuple_print(tab_name, cells)
        return None
    elif cmd_input.lower() == "exit;":
        return True
    else:
        print("Command \"{}\" not recognized".format(cmd_input))


def init():
    if os.path.exists('davisbase_columns.tbl'):
        pass
    else:
        init_file('davisbase_columns', True)
        fname = "davisbase_columns.tbl"
        davisbase_columns_schema = ['TEXT', 'TEXT', 'TEXT', 'TINYINT', 'TEXT', 'TEXT', 'TEXT']

        davisbase_columns_tuples = [["davisbase_tables", "rowid", "INT", 1, "NO", 'NO', 'NO' ],
                ["davisbase_tables", "tab_name", "TEXT", 2, "NO", 'NO', 'NO' ],
                 ["davisbase_columns", "rowid", "INT", 1, "NO", 'NO', 'NO' ],
                ["davisbase_columns", "tab_name", "TEXT", 2, "NO", 'NO', 'NO' ],
                ["davisbase_columns", "column_name", "TEXT", 3, "NO", 'NO', 'NO' ],
                ["davisbase_columns", "data_type", "TEXT", 4, "NO", 'NO', 'NO' ],
                ["davisbase_columns", "ordinal_position", "TINYINT", 5, "NO", 'NO', 'NO' ],
                ["davisbase_columns", "is_nullable", "TEXT", 6, "NO", 'NO', 'NO' ],
              ["davisbase_columns", "unique", "TEXT", 7, "NO", 'NO', 'NO' ],
              ["davisbase_columns", "primary_key", "TEXT", 8, "NO", 'NO', 'NO' ]]

        for i, tuple in enumerate(davisbase_columns_tuples):
            tuple = tab_create_tuple(davisbase_columns_schema, tuple, False, left_child_pg=None,  rowid=i+1)
            try:
                pg_insert_tuple(fname, 0, tuple)
            except:
                tab_leaf_split_pg(fname, 0, tuple)

    if os.path.exists('davisbase_tables.tbl'):
        pass
    else:
        init_file('davisbase_tables', True)
        fname = "davisbase_tables.tbl"
        davisbase_tables_schema = ['TEXT']

        cells = [["davisbase_tables"],
                ["davisbase_columns"]]
        for i, tuple in enumerate(cells):
            tuple = tab_create_tuple(davisbase_tables_schema, tuple, False, left_child_pg=None,  rowid=i+1)
            try:
                pg_insert_tuple(fname, 0, tuple)
            except:
                print("cell_size:",len(tuple))
                fbytes = load_file(fname)
                print("Remaining space in pg:", pg_available_bytes(fbytes, 0))


def print_help():
    print("DavisBase supported commands: (lowercase is all acceptable) \n1: SHOW TABLES;\n2: CREATE TABLE ...;\n3: DROP TABLE ...;\n4: INSERT INTO ...;\n5: SELECT ...;\n6: EXIT;")
    return None

def init_file(tab_name, is_tab, is_interior=False, right_child=0):
    if is_tab:
        ftype = ".tbl"
    else:
        ftype = '.ndx'
    if os.path.exists(tab_name+ftype):
        os.remove(tab_name+ftype)
    with open(tab_name+ftype, 'w+') as f:
        pass
    write_new_pg(tab_name, is_tab, is_interior, right_child, -1)
    return None

def init_indexes(column_dictionary):
    tab = list(column_dictionary.keys())
    tab_name = tab[0]
    col_names = list(column_dictionary[tab_name].keys())
    columns = list(column_dictionary[tab_name].values())
    for col in col_names:
        if column_dictionary[tab_name][col]['primary_key']=='YES':
            index_name = tab_name+'_'+col
            init_file(index_name, False)
    return None

def catalog_add_tab(column_dictionary):
    tab = list(column_dictionary.keys())
    assert(len(tab)==1)
    tab_name = tab[0].lower()
    columns =  column_dictionary[tab_name.upper()]
    col_names = list(columns.keys())
    tab_insert("davisbase_tables", [tab_name])
    tab_insert("davisbase_columns",[tab_name, "rowid", "INT", 1, "NO", 'NO', 'NO' ] )
    for col in col_names:
        values=[tab_name, col.lower(), columns[col]['data_type'].upper(), columns[col]['ordinal_position']+1, columns[col]['is_nullable'].upper(), columns[col]['unique'].upper(), columns[col]['primary_key'].upper()]
        tab_insert("davisbase_columns", values)


def write_new_pg(tab_name, is_tab, is_interior, right_sib_right_child, parent):
    assert(type(is_tab)==bool)
    assert(type(is_interior)==bool)
    assert(type(right_sib_right_child)==int)
    assert(type(parent)==int)
    is_leaf = not is_interior
    is_index = not is_tab
    if is_tab:
        ftype = ".tbl"
    else:
        ftype = '.ndx'
    file_size = os.path.getsize(tab_name + ftype)
    with open(tab_name + ftype, 'ab') as f:
        newpg = bytearray(SIZE_OF_PAGE*b'\x00')
        if is_tab and is_interior:
            newpg[0:1] = b'\x05'
        elif is_tab and is_leaf:
            newpg[0:1] = b'\x0d'
        elif is_index and is_interior:
            newpg[0:1] = b'\x02'
        elif is_index and is_leaf:
            newpg[0:1] = b'\x0a'
        else:
             raise ValueError("Page must be table/index")
        newpg[2:16] = struct.pack(endian+'hhii2x', 0, SIZE_OF_PAGE, right_sib_right_child, parent)
        f.write(newpg)
        assert(file_size%SIZE_OF_PAGE==0)
        return int(file_size/SIZE_OF_PAGE)

def datatype_to_int(datatype):
    datatype = datatype.lower()
    mapping = {"null":0,"tinyint":1, "smallint":2, "int":3, "bigint":4, "long":4, 'float':5, "double":6, "year":8, "time":9, "datetime":10, "date":11, "text":12}
    return mapping[datatype]

def int_to_fstring(key):
    int2packstring={
    2:'h', 3:'i', 4:'q', 5:'f', 6:'d',
    9:'i', 10:'Q', 11:'Q' }
    return int2packstring[key]

def schema_to_int(schema, values):
    datatypes = [datatype_to_int(datatype1) for datatype1 in schema]
    for i, val in enumerate(values):
        if val==None:
            datatypes[i]=0
            continue
        elif datatypes[i]==12:
            datatypes[i]+=len(val)
    return datatypes

def get_datatype1_size(datatype1):
    if datatype1==0:
        return 0
    if datatype1 in [1,8]:
        return 1
    elif datatype1 in [2]:
        return 2
    elif datatype1 in [3,5,9]:
        return 4
    elif datatype1 in [4,6,10,11]:
        return 8
    elif datatype1>=12:
        return datatype1-12
    else:
        raise ValueError("datatype issue")

def date_to_bytes(date, time=False):
    if not time:
        return struct.pack(">q", int(round(date.timestamp() * 1000)))
    else:
        return struct.pack(">i", int(round(date.timestamp() * 1000)))

def bytes_to_dates(bt, time=False):
    if not time:
        return datetime.fromtimestamp((struct.unpack(">q", bt)[0])/1000)
    else:
        return datetime.fromtimestamp((struct.unpack(">i", bt)[0])/1000)

def time_to_byte(t):
    d =  datetime(1970,1,2,t.hour,t.minute, t.microsecond)
    return date_to_bytes(d, time=True)

def byte_to_time(bt):
    return bytes_to_dates(bt, time=True).time()

def val_datatype_to_byte(val, datatype1):
    if val == None:
        return b''
    if datatype1==1:
        return val.to_bytes(1, byteorder=sys.byteorder, signed=True)
    if datatype1==8:
        return (val-2000).to_bytes(1, byteorder=sys.byteorder, signed=True)
    if datatype1 in [2,3,4,5,6]:
        return struct.pack(int_to_fstring(datatype1), val)
    if datatype1 in [10,11]:
        return date_to_bytes(val)
    if datatype1==9:
        return time_to_byte(val)
    elif datatype1>=12:
        return val.encode('ascii')


def datatype_byte_to_val(datatype1, byte_str):
    if datatype1==0:
        return None
    elif datatype1==1:
        return int.from_bytes(byte_str, byteorder=sys.byteorder, signed=False)
    elif datatype1==8:
        return int.from_bytes(byte_str, byteorder=sys.byteorder, signed=False)+2000
    elif datatype1 in [2,3,4,5,6]:
        return struct.unpack(int_to_fstring(datatype1), byte_str)[0]
    if datatype1 in [10,11]:
        return bytes_to_dates(byte_str)
    if datatype1==9:
        return byte_to_time(byte_str)
    elif datatype1>=12:
        return byte_str.decode("utf-8")
    else:
         raise ValueError("datatype_byte error ")


def tab_values_to_result(schema, value_list):
    datatypes = schema_to_int(schema, value_list)
    byte_string = b''
    for val, datatype1 in zip(value_list, datatypes):
        byte_val = val_datatype_to_byte(val, datatype1)

        byte_string += byte_val
    return byte_string, datatypes


def tab_result_to_values(query_result):
    num_columns = query_result[0]
    temp = query_result[1:]
    datatypes =  temp[:num_columns]
    temp = temp[num_columns:]
    i = 0
    values = []
    for datatype1 in datatypes:
        element_size = get_datatype1_size(datatype1)
        byte_str = temp[i:i+element_size]
        values.append(datatype_byte_to_val(datatype1, byte_str))
        i+=element_size
    assert(i==len(temp))
    return values

def index_datatype_value_rowids_to_result(index_datatype, index_value, rowid_list):
    datatype1 = schema_to_int([index_datatype], [index_value])
    bin_num_assoc_rowids = bytes([len(rowid_list)])
    bin_ind_datatype = bytes(datatype1)
    bin_index_val = val_datatype_to_byte(index_value, *datatype1)
    bin_rowids = struct.pack(endian+str(len(rowid_list))+'i', *rowid_list)
    query_result = bin_num_assoc_rowids + bin_ind_datatype + bin_index_val+bin_rowids
    return query_result


def index_result_to_values(query_result):
    assoc_row_ids = query_result[0]
    ind_datatype = query_result[1]
    element_size = get_datatype1_size(ind_datatype)
    ind_byte_str = query_result[2:2+element_size]
    ind_value = datatype_byte_to_val(ind_datatype, ind_byte_str)
    bin_rowid_list  = query_result[2+element_size:]
    i=0
    j = len(bin_rowid_list)
    rowid_values = []
    while(i<j):
        rowid_values.append(struct.unpack(endian+'i', bin_rowid_list[i:i+4])[0])
        i+=4
    return ind_value, rowid_values

def tab_create_tuple(schema, value_list, is_interior, left_child_pg=None,  rowid=None):
    assert(len(value_list)==len(schema))
    assert(type(schema)==list)
    assert(type(value_list)==list)
    assert(type(is_interior)==bool)

    if  is_interior:
        assert(left_child_pg != None)
        assert(rowid != None)
        tuple = struct.pack(endian+'ii', left_child_pg, rowid)

    else:
        assert(rowid != None)
        query_result_body, datatypes  = tab_values_to_result(schema, value_list)
        query_result_header = bytes([len(datatypes)]) + bytes(datatypes)
        cell_result = query_result_header + query_result_body
        cell_header = struct.pack(endian+'hi', len(cell_result), rowid)
        tuple = cell_header + cell_result

    return tuple


def index_create_tuple(index_datatype, index_value, rowid_list, is_interior, left_child_pg=None):
    assert(type(is_interior)==bool)
    is_leaf = not is_interior

    query_result = index_datatype_value_rowids_to_result(index_datatype, index_value, rowid_list)
    if is_interior:
        assert(left_child_pg != None)
        cell_header = struct.pack(endian+'IH', left_child_pg, len(query_result))
    elif is_leaf:
        cell_header = struct.pack(endian+'H', len(query_result))
    else:
         raise ValueError("Page must be either table")

    tuple = cell_header + query_result
    return tuple


def tab_read_tuple(tuple, is_interior):
    is_leaf = not is_interior
    if  is_interior:
        cell_header = struct.unpack(endian+'ii', tuple[0:8])
        res = {'left_child_pg':cell_header[0],'rowid':cell_header[1]}
    elif is_leaf:
        cell_header = struct.unpack(endian+'hi', tuple[0:6])
        query_result = tuple[6:]
        values = tab_result_to_values(query_result)
        res = {'bytes':cell_header[0]+6, 'rowid':cell_header[1],"data":values}
    else:
        print("error in read tuple")
    res["cell_size"]=len(tuple)
    res['cell_binary'] = tuple
    return res

def index_read_tuple(tuple, is_interior):
    result=dict()
    if  is_interior:
        cell_header = struct.unpack(endian+'ih', tuple[0:6])
        result["left_child_pg"]=cell_header[0]
        result["bytes"]=cell_header[0]+6
        query_result = tuple[6:]
    else:
        cell_header = struct.unpack(endian+'h', tuple[0:2])
        result["bytes"]=cell_header[0]+6
        query_result = tuple[2:]

    ind_value, rowid_list = index_result_to_values(query_result)
    result["index_value"]=ind_value
    result["assoc_rowids"]=rowid_list
    result["cell_size"]=len(tuple)
    result['cell_binary'] = tuple
    return result


def save_pg(fname, pg_num, new_pg_data):
    assert(len(new_pg_data)==SIZE_OF_PAGE)
    foffset = pg_num*SIZE_OF_PAGE
    foffset_end = (pg_num+1)*SIZE_OF_PAGE
    fbytes = load_file(fname)
    fbytes = bytearray(fbytes)
    fbytes[foffset:foffset_end] = new_pg_data
    with open(fname, 'r+b') as f:
        f.seek(0)
        pg = f.write(fbytes)
    return None


def pg_available_bytes(fbytes, pg_num):
    pg = load_pg(fbytes, pg_num)
    number_tuples = struct.unpack(endian+'h', pg[2:4])[0]
    bytes_from_top = 16+(2*number_tuples)
    cell_content_start =struct.unpack(endian+'h', pg[4:6])[0]
    return  cell_content_start - bytes_from_top


def shift_pg_content(pg, top_ind, bot_ind, steps_to_shift, up=True):
    if steps_to_shift==0:
        return pg
    copy = pg[top_ind:bot_ind]
    if up:
        assert(top_ind-steps_to_shift>=0)
        new_top_ind = top_ind - steps_to_shift
        new_bot_ind = bot_ind - steps_to_shift
        pg[new_top_ind:new_bot_ind]=copy
        pg[new_bot_ind:bot_ind]=b'\x00'*steps_to_shift
        return pg
    else:
        assert(bot_ind+steps_to_shift<=SIZE_OF_PAGE)
        new_top_ind = top_ind + steps_to_shift
        new_bot_ind = bot_ind + steps_to_shift
        pg[new_top_ind:new_bot_ind]=copy
        pg[top_ind:new_top_ind]=b'\x00'*steps_to_shift
        return pg

def update_array_values(pg, first_array_loc_to_change, number_tuples, steps_to_shift, up=True):
    if steps_to_shift==0:
        return pg
    if up:
        for i in range(first_array_loc_to_change, number_tuples):
            top_of_array = 16+2*i
            bot_of_array = 16+2*(i+1)
            prev_val = struct.unpack(endian+'h',pg[top_of_array:bot_of_array])[0]
            pg[top_of_array:bot_of_array]=struct.pack(endian+'h', prev_val-steps_to_shift)
    else:
        for i in range(first_array_loc_to_change, number_tuples):
            top_of_array = 16+2*i
            bot_of_array = 16+2*(i+1)
            prev_val = struct.unpack(endian+'h',pg[top_of_array:bot_of_array])[0]
            pg[top_of_array:bot_of_array]=struct.pack(endian+'h', prev_val+steps_to_shift)
    return pg


def get_tuple_indices(pg, cell_ind):
    cell_top_idx = struct.unpack(endian+'h',pg[16+2*cell_ind:16+2*(cell_ind+1)])[0]
    if cell_ind==0:
        cell_bot_idx = SIZE_OF_PAGE
    else:
        cell_bot_idx = struct.unpack(endian+'h',pg[16+2*(cell_ind-1):16+2*(cell_ind)])[0]
    return cell_top_idx, cell_bot_idx

def pg_delete_tuple(fname, pg_num, cell_ind):
    fbytes = load_file(fname)
    pg = load_pg(fbytes, pg_num)
    pg = bytearray(pg)
    number_tuples = struct.unpack(endian+'h', pg[2:4])[0]
    assert(cell_ind<=number_tuples-1)
    assert(number_tuples>=1)
    assert(cell_ind>=0)

    cell_content_area_start = struct.unpack(endian+'h', pg[4:6])[0]
    array_end = 16+2*number_tuples
    array_idx_top = 16+2*cell_ind
    array_idx_bot = 16+2*(cell_ind+1)


    if (cell_ind==number_tuples-1) & (cell_ind!=0):
        cell_top_loc, cell_bot_loc = get_tuple_indices(pg, cell_ind)
        cell_2_delete = pg[cell_top_loc:cell_bot_loc]
        dis2replace= len(cell_2_delete)
        pg[cell_top_loc:cell_bot_loc]=b'\x00'*dis2replace
        pg[4:6] = struct.pack(endian+'h', cell_content_area_start+dis2replace)
        pg[16+2*(number_tuples-1):16+2*(number_tuples)]=b'\x00'*2
        pg[2:4] = struct.pack(endian+'h', number_tuples-1)
    else:
        cell_top_loc, cell_bot_loc = get_tuple_indices(pg, cell_ind)
        cell_2_delete = pg[cell_top_loc:cell_bot_loc]
        dis2replace= len(cell_2_delete)
        pg = shift_pg_content(pg, cell_content_area_start, cell_top_loc, dis2replace, up=False)
        pg = update_array_values(pg, cell_ind, number_tuples, dis2replace, up=False)
        pg[4:6] = struct.pack(endian+'h', cell_content_area_start+dis2replace)
        pg = shift_pg_content(pg, array_idx_bot, array_end, 2, up=True)
        pg[2:4] = struct.pack(endian+'h', number_tuples-1)
    save_pg(fname, pg_num, pg)
    assert(len(pg)==SIZE_OF_PAGE)
    return (number_tuples - 1) == 0


def pg_update_tuple(fname, pg_num, cell_ind, tuple):
    fbytes = load_file(fname)
    pg = load_pg(fbytes, pg_num)
    pg = bytearray(pg)

    number_tuples = struct.unpack(endian+'h', pg[2:4])[0]
    assert(cell_ind<=number_tuples-1)
    assert(number_tuples!=0)
    assert(cell_ind>=0)

    cell_content_area_start = struct.unpack(endian+'h', pg[4:6])[0]
    array_end = 16+2*number_tuples
    array_idx_top = 16+2*cell_ind
    array_idx_bot = 16+2*(cell_ind+1)
    available_bytes = pg_available_bytes(fbytes, pg_num)
    cell_top_idx, cell_bot_idx = get_tuple_indices(pg, cell_ind)
    cell_2_update = pg[cell_top_idx:cell_bot_idx]
    if len(cell_2_update)==len(tuple):
        pg[cell_top_idx:cell_bot_idx] = tuple
    elif len(cell_2_update)<len(tuple):
        dis2move =  len(tuple) - len(cell_2_update)
        assert(dis2move<=available_bytes)
        pg = shift_pg_content(pg, cell_content_area_start, cell_top_idx, dis2move, up=True)
        pg = update_array_values(pg, cell_ind, number_tuples, dis2move, up=True)
        pg[4:6] = struct.pack(endian+'h', cell_content_area_start-dis2move)
        pg[cell_top_idx-dis2move:cell_bot_idx] = tuple
    else:
        dis2move =  len(cell_2_update) - len(tuple)
        pg = shift_pg_content(pg, cell_content_area_start, cell_top_idx, dis2move, up=False)
        pg = update_array_values(pg, cell_ind, number_tuples, dis2move, up=True)
        pg[4:6] = struct.pack(endian+'h', cell_content_area_start+dis2move)
        pg[cell_top_idx+dis2move:cell_bot_idx] = tuple
    save_pg(fname, pg_num, pg)
    assert(len(pg)==SIZE_OF_PAGE)
    return None

def update_pg_header(fname, pg_num, right_sib_right_child=None, is_interior=None, parent=None):
    is_tab = fname[-4:]=='.tbl'
    is_index=not is_tab
    is_leaf = not is_interior
    fbytes = load_file(fname)
    pg = load_pg(fbytes, pg_num)
    pg = bytearray(pg)
    if right_sib_right_child is not None:
        assert(len(fbytes)/SIZE_OF_PAGE>=right_sib_right_child)
        pg[6:10] = struct.pack(endian+'i', right_sib_right_child)
    if is_interior is not None:
        if pg[0] in [5,13]:
            is_tab = True
        else:
            is_tab = False
        if is_tab and is_interior:
            pg[0:1] = b'\x05'
        elif is_tab and is_leaf:
            pg[0:1] = b'\x0d'
        elif is_index and is_interior:
            pg[0:1] = b'\x02'
        elif is_index and is_leaf:
            pg[0:1] = b'\x0a'
    if parent is not None:
        pg[10:14] = struct.pack(endian+'i', parent)
    save_pg(fname, pg_num, pg)
    return None

def update_tuple_leftpointer(fname, pg_num, cell_ind, lpointer=None, rowid=None):
    fbytes = load_file(fname)
    pg = load_pg(fbytes, pg_num)
    pg = bytearray(pg)
    cell_top_idx, cell_bot_idx = get_tuple_indices(pg, cell_ind)
    if lpointer!=None:
        pg[cell_top_idx:cell_top_idx+4] = struct.pack(endian+'i', lpointer)
    if rowid!=None:
        pg[cell_top_idx+4:cell_top_idx+8] = struct.pack(endian+'i', rowid)
    save_pg(fname, pg_num, pg)
    return None


def load_file(fname):
    with open(fname, 'rb') as f:
        return f.read()

def load_pg(fbytes, pg_num):
    foffset = pg_num*SIZE_OF_PAGE
    return fbytes[foffset:(pg_num+1)*SIZE_OF_PAGE]

def read_tuples_in_pg(fbytes, pg_num):
    assert(pg_num<(len(fbytes)/SIZE_OF_PAGE))
    pg = load_pg(fbytes, pg_num)
    number_tuples = struct.unpack(endian+'h', pg[2:4])[0]
    parent_pg = struct.unpack(endian+'i', pg[10:14])[0]
    available_bytes = pg_available_bytes(fbytes, pg_num)
    if pg[0] in [5,13]:
        is_tab = True
    else:
        is_tab = False

    if pg[0] in [2,5]:
        is_interior = True
    else:
        is_interior = False

    i=0
    data = []
    while i<=number_tuples-1:
        if i == 0:
            cell_bot_loc = SIZE_OF_PAGE
        else:
            cell_bot_loc = struct.unpack(endian+'h',pg[16+2*(i-1):16+2*(i)])[0]
        cell_top_loc = struct.unpack(endian+'h',pg[16+2*i:16+2*(i+1)])[0]
        tuple = pg[cell_top_loc:cell_bot_loc]
        if is_tab:
            data.append(tab_read_tuple(tuple, is_interior))
        else:
            data.append(index_read_tuple(tuple, is_interior))
        i+=1

    result = {
    "pg_number":pg_num,
    "parent_pg":parent_pg,
    "is_tab": is_tab,
    "is_leaf": not is_interior,
    "number_tuples":number_tuples,
    "available_bytes":available_bytes
    }
    if is_interior:
        result['rightmost_child_pg'] = parent_pg = struct.unpack(endian+'i', pg[6:10])[0]
    else:
        result['right_sibling_pg'] = parent_pg = struct.unpack(endian+'i', pg[6:10])[0]
    result['cells']=data
    if is_tab:
        result['rowids'] = [i['rowid'] for i in data]
    else:
        result['index_values'] = [i['index_value'] for i in data]
    return result


def read_all_pgs_in_file(fname):
    if fname[-3:]=='tbl':
        is_tab=True
    else:
        is_tab = False

    file = load_file(fname)
    file_size = len(file)
    assert(file_size%SIZE_OF_PAGE==0)
    num_pgs = int(file_size/SIZE_OF_PAGE)
    data = []
    for pg_num in range(num_pgs):
        data.append(read_tuples_in_pg(file, pg_num))
    for pg in data:
        if pg['is_leaf']:
            if pg['right_sibling_pg']!=-1:
                if data[pg['right_sibling_pg']]['parent_pg']==pg['parent_pg']:
                    data[pg['right_sibling_pg']]['left_sibling_pg'] = pg['pg_number']
        else:
            for i, tuple in enumerate(pg['cells']):
                child_pg = tuple['left_child_pg']
                if i!=0:
                    data[child_pg]['left_sibling_pg']=pg['cells'][i-1]['left_child_pg']
                if i+1!=len(pg['cells']):
                    data[child_pg]['right_sibling_pg']=pg['cells'][i+1]['left_child_pg']
                else:
                    data[child_pg]['right_sibling_pg']=pg['rightmost_child_pg']
            data[pg['rightmost_child_pg']]['left_sibling_pg']=pg['cells'][-1]['left_child_pg']
            data[pg['rightmost_child_pg']]['right_sibling_pg']=-1


    return data


def get_indexes(tab_name):
    indexes=[]
    for filename in os.listdir():
        if (filename[:len(tab_name)]==tab_name) and (filename[-4:]=='.ndx'):
            indexes.append(filename)
    return indexes


def get_next_pg_rowid(tab_name):
    pgs = read_all_pgs_in_file(tab_name+'.tbl')
    final_pg_num = 0
    while not pgs[final_pg_num]['is_leaf']:
        final_pg_num = pgs[final_pg_num]['rightmost_child_pg']

    final_pg = pgs[final_pg_num]
    if len(pgs[0]['cells'])==0:
        next_rowid=0
    else:
        rowid_sorted_tuples = sorted(final_pg['cells'], key=lambda x: x['rowid'])
        next_rowid = rowid_sorted_tuples[-1]['rowid']
    return final_pg['pg_number'], next_rowid + 1


def get_col_names_from_catalog(tab_name):
    schema, catalog_tuples = catalog_schema(tab_name, with_rowid=True)
    col_names = []
    for tuple in catalog_tuples:
        col_names.append((tuple['data'][3],tuple['data'][1]))
    col_names = sorted(col_names, key=lambda x: x[0])
    return  [i[1] for i in col_names]


def catalog_schema(tab_name, with_rowid=False):
    data = read_all_pgs_in_file('davisbase_columns.tbl')
    all_tuples = []
    all_data = []
    for pg in data:
        if not pg['is_leaf']:
            continue
        for tuple in pg['cells']:
            col_tab = tuple['data'][0].lower()
            if col_tab==tab_name.lower():
                col_name = tuple['data'][1].lower()
                if col_name=='rowid' and not with_rowid:
                    continue
                all_tuples.append((tuple['data'][3],tuple['data'][2]))
                all_data.append(tuple)
    all_tuples = sorted(all_tuples, key=lambda x: x[0])
    schema = [i[1] for i in all_tuples]
    return schema, all_data

def index_insert_tuple_in_pg(fname, pg_num, tuple, cell_ind):
    fbytes = load_file(fname)
    pg = load_pg(fbytes, pg_num)
    pg = bytearray(pg)

    number_tuples = struct.unpack(endian+'h', pg[2:4])[0]
    if cell_ind == number_tuples:
        pg_insert_tuple(fname, pg_num, tuple)
        return None

    assert(cell_ind<=number_tuples-1)
    assert(cell_ind>=0)
    assert(len(tuple)<pg_available_bytes(fbytes, pg_num))
    cell_content_area_start = struct.unpack(endian+'h', pg[4:6])[0]
    array_end = 16+2*number_tuples
    array_idx_top = 16+2*cell_ind
    array_idx_bot = 16+2*(cell_ind+1)

    cell_top_loc, cell_bot_loc = get_tuple_indices(pg, cell_ind)
    dis2move= len(tuple)
    pg = shift_pg_content(pg, cell_content_area_start, cell_bot_loc, dis2move, up=True)
    pg = update_array_values(pg, cell_ind, number_tuples, dis2move, up=True)
    pg[4:6] = struct.pack(endian+'h', cell_content_area_start-dis2move)
    pg = shift_pg_content(pg, array_idx_top, array_end, 2, up=False)
    pg[array_idx_top:array_idx_bot] = struct.pack(endian+'h', cell_bot_loc-dis2move)
    pg[cell_bot_loc-dis2move:cell_bot_loc] = tuple
    pg[2:4] = struct.pack(endian+'h', number_tuples+1)
    assert(len(pg)==SIZE_OF_PAGE)
    save_pg(fname, pg_num, pg)
    return (number_tuples - 1) == 0


def print_it(fname, pg_format=False, limit=None, pgs=None):
    if pgs ==None:
        pgs  =read_all_pgs_in_file(fname)
    print(fname[:-4].upper())
    if pg_format:
        for pg in pgs:
            if pg["is_leaf"]:
                continue
            else:
                print()
                print("pg_number: ",pg['pg_number'])
                print("parent_pg: ",pg['parent_pg'])
                print("right_child_pg: ",pg['rightmost_child_pg'])
                print("bytes remaining:", pg['available_bytes'])
                for tuple in pg["cells"]:
                    if fname[-4:]=='.tbl':
                        print("rowid: ",tuple['rowid'],"left child: ",tuple['left_child_pg'])
                    else:
                        print("ind_val: ",tuple['index_value'],"left child: ",tuple['left_child_pg'])
        for pg in pgs:
            if not pg["is_leaf"]:
                continue
            else:
                print()
                print("pg_number: ",pg['pg_number'])
                print("parent_pg: ",pg['parent_pg'])
                print("right_sibling_pg: ",pg['right_sibling_pg'])
                print("bytes remaining:", pg['available_bytes'])
                rowids = []
                for tuple in pg["cells"]:
                    if fname[-4:]=='.tbl':
                        rowids.append(tuple['rowid'])
                    else:
                        rowids.append(tuple['index_value'])
                print(rowids)
    else:
        rows = []
        for pg in pgs:
            if not pg["is_leaf"]:
                continue
            else:
                for tuple in pg["cells"]:
                    if fname[-4:]=='.tbl':
                        rows.append([tuple['rowid']]+tuple['data'])
                    else:
                        rows.append([tuple['index_value'],tuple['assoc_rowids']])
        rows = sorted(rows, key=lambda x: x[0])
        i=1
        for row in rows:
            if limit!=None and i>limit:
                break
            print(row)
            i+=1

def add_rowid_to_tuple(fname, pg_num, cell_ind, rowid, tuple):
    cell_binary = tuple['cell_binary']+struct.pack(endian+'i', rowid)
    try:
        pg_update_tuple(fname, pg_num, cell_ind, cell_binary)
    except:
        return

def get_all_tab_tuples(tab_name):
    pgs  =read_all_pgs_in_file(tab_name+'.tbl')
    cells = []
    for pg in pgs:
        if not pg["is_leaf"]:
            continue
        else:
            for tuple in pg["cells"]:
                cells.append(tuple)
    return cells

def create_tab(cmd_input):
    col_catalog_dictionary = parse_create_tab(cmd_input)
    tab_name = list(col_catalog_dictionary.keys())[0]
    tab_name= tab_name.lower()
    if os.path.exists(tab_name+'.tbl'):
        print("Table {} already exists.".format(tab_name))
        return None
    init_file(tab_name, True)
    catalog_add_tab(col_catalog_dictionary)
    init_indexes(col_catalog_dictionary)
    return None

def create_index(cmd_input):
    tab_name, column_name = parse_create_index(cmd_input)
    index_name = tab_name+'_'+column_name
    init_file(index_name, False)
    columns = get_col_names_from_catalog(tab_name)[1:]
    schema, _ = catalog_schema(tab_name)
    ord_position =  columns.index(column_name)
    index_datatype = schema[ord_position]
    cells = get_all_tab_tuples(tab_name)
    for tuple in cells:
        rowid = tuple['rowid']
        index_value = tuple['data'][ord_position]
        index_insert(tab_name, column_name, index_datatype, index_value, rowid)

def insert_into(cmd_input):
    tab_name, values = parse_insert_into(cmd_input)
    tab_name = tab_name.lower()
    violation_flag = False
    if violation_flag:
        print("Constraint violated for row {}".format(violating_row))
        return None
    schema, all_col_data = catalog_schema(tab_name)
    col_names = get_col_names_from_catalog(tab_name)[1:]
    indexes = get_indexes(tab_name)
    for val in values:
        next_pg, next_rowid = get_next_pg_rowid(tab_name)
        tuple = tab_create_tuple(schema, val, False,  rowid=next_rowid)
        try:
            pg_insert_tuple(tab_name+'.tbl', next_pg, tuple)
        except:
            tab_leaf_split_pg(tab_name+'.tbl', next_pg, tuple)
        for filename in indexes:
            index_colname = filename[len(tab_name)+1:-4]
            i = col_names.index(index_colname.lower())
            index_datatype= schema[i]
            index_value= val[i]
            index_insert(tab_name, index_colname, index_datatype, index_value, next_rowid)


def drop_tab(cmd_input):
    tab_name = parse_drop_tab(cmd_input)
    tab_name = tab_name.lower()
    if os.path.exists(tab_name+".tbl"):
        os.remove(tab_name+".tbl")
        _, rows = catalog_schema(tab_name, with_rowid=True)
        rowids = [row['rowid'] for row in rows]
        tab_delete('davisbase_columns.tbl', rowids)
        data = read_all_pgs_in_file('davisbase_tables.tbl')
        for pg in data:
            if not pg['is_leaf']:
                continue
            for tuple in pg['cells']:
                if tab_name==tuple['data'][0].lower():
                    rowids = [tuple['rowid']]
                    break
        tab_delete('davisbase_tables.tbl', rowids)
        for index in get_indexes(tab_name.upper()):
            os.remove(index)
    else:
        print("Table \"{}\" does not exist.".format(tab_name))

def show_tabs():
    print_it("davisbase_tables.tbl", pg_format=False, limit=None)
    return None

def index_insert(tab_name, column_name, index_datatype, index_value, rowid):
    fname = tab_name+'_'+column_name+'.ndx'
    pgs = read_all_pgs_in_file(fname)
    pg_num, cell_ind = pg_tuple_ind_given_key(pgs, index_value)
    pg = pgs[pg_num]
    if len(pg['cells'])!=cell_ind:
        tuple = pg['cells'][cell_ind]
        if tuple['index_value']==index_value:
            if rowid not in tuple['assoc_rowids']:
                add_rowid_to_tuple(fname, pg_num, cell_ind, rowid, tuple)
                return
    tuple = index_create_tuple(index_datatype, index_value, [rowid], False, left_child_pg=None)
    if pgs[pg_num]['available_bytes']/SIZE_OF_PAGE<0.5:
        index_leaf_split_pg(fname, pg_num, tuple, index_datatype, cell_ind)
        return
    else:
        try:
            index_insert_tuple_in_pg(fname, pg_num, tuple, cell_ind)
        except:
            index_leaf_split_pg(fname, pg_num, tuple, index_datatype, cell_ind)

def index_interior_split_pg(fname, split_pg_num, cell2insert, new_rightmost_pg, cell_index):
    pgs = read_all_pgs_in_file(fname)
    values = pgs[split_pg_num]
    tab_name = fname[:-4]
    parent_num = values['parent_pg']
    is_interior = not values['is_leaf']
    is_tab = values['is_tab']
    assert(not is_tab)
    assert(is_interior)
    number_tuples = values['number_tuples']
    cells = values['cells']
    mid_tuple = int((number_tuples+1)//2)
    cell2insert=index_read_tuple(cell2insert, is_interior)
    insert_order = []
    i=0
    copied=False
    while len(insert_order)!=len(cells)+1:
        if cell_index==i and not copied:
            insert_order.append(cell2insert)
            copied = True
        else:
            insert_order.append(cells[i])
            i+=1
    mid_index = insert_order[mid_tuple]['index_value']
    rightmost_child_pg_right = values['rightmost_child_pg']
    rightmost_child_pg_left = insert_order[mid_tuple]['left_child_pg']
    copyoftuples = [tuple['cell_binary'] for tuple in insert_order]
    mid_tuple_binary = copyoftuples[mid_tuple]
    if parent_num==-1:
        right_child_num = write_new_pg(tab_name, is_tab, is_interior, new_rightmost_pg, split_pg_num)
        left_child_num = write_new_pg(tab_name, is_tab, is_interior, rightmost_child_pg_left, split_pg_num)
        for order_tuple in insert_order[:mid_tuple+1]:
            update_pg_header(fname, order_tuple['left_child_pg'], parent=left_child_num)
        pg_insert_tuple(fname, left_child_num, copyoftuples[:mid_tuple])
        for order_tuple in insert_order[mid_tuple+1:]:
            update_pg_header(fname, order_tuple['left_child_pg'], parent=right_child_num)
        update_pg_header(fname, values['rightmost_child_pg'], parent=right_child_num)
        pg_insert_tuple(fname, right_child_num, copyoftuples[mid_tuple+1:])
        pg_delete_tuples_on_and_after(fname, split_pg_num, 0)
        pg_insert_tuple(fname, split_pg_num, mid_tuple_binary)
        update_tuple_leftpointer(fname, split_pg_num, 0, left_child_num)
        update_pg_header(fname, split_pg_num, right_sib_right_child=right_child_num)
        print_it("tab_name_column1.ndx", pg_format=True)
        return right_child_num
    else:
        right_sib = write_new_pg(tab_name, is_tab, is_interior, new_rightmost_pg, parent_num)
        update_pg_header(fname, split_pg_num, right_sib_right_child=rightmost_child_pg_left)
        pg_delete_tuples_on_and_after(fname, split_pg_num, 0)
        for order_tuple in insert_order[:mid_tuple+1]:
            update_pg_header(fname, order_tuple['left_child_pg'], parent=split_pg_num)
        pg_insert_tuple(fname, split_pg_num, copyoftuples[:mid_tuple])
        for order_tuple in insert_order[mid_tuple+1:]:
            update_pg_header(fname, order_tuple['left_child_pg'], parent=right_sib)
        update_pg_header(fname, values['rightmost_child_pg'], parent=right_sib)
        pg_insert_tuple(fname, right_sib, copyoftuples[mid_tuple+1:])
        parent_pg = pgs[parent_num]
        parent_tuples = parent_pg['cells']
        for i, tuple in enumerate(parent_tuples)-1:
            if tuple['index_value'] >  mid_index:
                parent_index = i
                update_tuple_leftpointer(fname, parent_num, i, right_sib)
                break
            elif i==len(parent_tuples):
                parent_index = len(parent_tuples)
                update_pg_header(fname, parent_num, right_sib_right_child=right_sib)
        mid_tuple_binary = bytearray(mid_tuple_binary)
        mid_tuple_binary[0:4] = struct.pack(endian+'i', split_pg_num)
        if parent_pg['available_bytes']/SIZE_OF_PAGE<0.5:
            new_parent = index_interior_split_pg(fname, parent_num, mid_tuple_binary, right_sib, parent_index)
        else:
            try:
                index_insert_tuple_in_pg(fname, parent_num, mid_tuple_binary, parent_index)
            except:
                new_parent = index_interior_split_pg(fname, parent_num, mid_tuple_binary, right_sib, parent_index)
        return right_sib

def index_leaf_split_pg(fname, split_pg_num, cell2insert, index_datatype, cell_index):
    fbytes = load_file(fname)
    values = read_tuples_in_pg(fbytes, split_pg_num)
    tab_name = fname[:-4]
    parent_num = values['parent_pg']
    is_interior = not values['is_leaf']
    is_tab = values['is_tab']
    assert(not is_tab)
    number_tuples = values['number_tuples']
    cells = values['cells']
    mid_tuple = int((number_tuples+1)/2)
    cell2insert=index_read_tuple(cell2insert, is_interior)
    insert_order = []
    i=0
    copied=False
    while len(insert_order)!=len(cells)+1:
        if cell_index==i and not copied:
            insert_order.append(cell2insert)
            copied = True
        else:
            insert_order.append(cells[i])
            i+=1

    mid_index = insert_order[mid_tuple]['index_value']
    copyoftuples = [tuple['cell_binary'] for tuple in insert_order]
    mid_tuple_binary = copyoftuples[mid_tuple]

    if parent_num==-1:
        right_child_num = write_new_pg(tab_name, is_tab, False, -1, split_pg_num)
        left_child_num = write_new_pg(tab_name, is_tab, False, right_child_num, split_pg_num)
        pg_insert_tuple(fname, left_child_num, copyoftuples[:mid_tuple])
        pg_insert_tuple(fname, right_child_num, copyoftuples[mid_tuple+1:])
        pg_delete_tuples_on_and_after(fname, split_pg_num, 0)
        mid_tuple_binary = struct.pack(endian+'i', left_child_num) + mid_tuple_binary
        pg_insert_tuple(fname, split_pg_num, mid_tuple_binary)
        update_pg_header(fname, split_pg_num, right_sib_right_child=right_child_num, is_interior=True)

    else:
        right_sibling_pg = values['right_sibling_pg']
        right_sib = write_new_pg(tab_name, is_tab, is_interior, right_sibling_pg, parent_num)
        pg_delete_tuples_on_and_after(fname, split_pg_num, 0)
        pg_insert_tuple(fname, split_pg_num, copyoftuples[:mid_tuple])
        update_pg_header(fname, split_pg_num, right_sib_right_child=right_sib)
        pg_insert_tuple(fname, right_sib, copyoftuples[mid_tuple+1:])
        mid_tuple_binary = struct.pack(endian+'i', split_pg_num) + mid_tuple_binary
        parent_pg = read_tuples_in_pg(fbytes, parent_num)
        parent_tuples = parent_pg['cells']

        for i, tuple in enumerate(parent_tuples):
            if tuple['index_value'] >  mid_index:
                parent_index = i
                update_tuple_leftpointer(fname, parent_num, i, right_sib)
                break
            elif i==len(parent_tuples)-1:
                parent_index = len(parent_tuples)
                update_pg_header(fname, parent_num, right_sib_right_child=right_sib)

        if parent_pg['available_bytes']/SIZE_OF_PAGE<0.5:
            new_parent = index_interior_split_pg(fname, parent_num, mid_tuple_binary, right_sib,parent_index)
            return None
        else:
            try:
                index_insert_tuple_in_pg(fname, parent_num, mid_tuple_binary, parent_index)
            except:
                new_parent = index_interior_split_pg(fname, parent_num, mid_tuple_binary, right_sib, parent_index)
        try:
            validate(fname)
        except:
            print("splitting",right_sib, split_pg_num)
            assert(False)

def delete(tab_name, rowid):
    pg_num, cell_ind = pg_tuple_ind_given_index_value(tab_name, rowid)
    if cell_ind is None:
        return None
    else:
        try:
            pg_delete_tuple(fname, pg_num, cell_ind)
        except:
            tab_leaf_merge_pg(tab_name+'.tbl', next_pg, tuple)

        for filename in get_indexes(tab_name):
            for col in all_col_data:
                if col['data'][1]==fname[len(tab_name)+1:-4]:
                    index_datatype= col['data'][2]
                    index_value= values[col['data'][3]]
            next_pg  = index_get_next_pg(index_value)
            try:
                index_pg_delete_tuple(index_datatype, index_value, next_rowid)
            except:
                index_leaf_merge_pg(tab_name+'.tbl', next_pg, tuple)
        return None

def pg_insert_tuple(fname, pg_num, tuple):
    fbytes = load_file(fname)
    pg = load_pg(fbytes, pg_num)
    pg = bytearray(pg)

    if type(tuple)==list:
        cells = tuple
        for tuple in cells:
            assert(len(tuple)<pg_available_bytes(fbytes, pg_num))
            number_tuples = struct.unpack(endian+'h', pg[2:4])[0]
            bytes_from_top = 16+(2*number_tuples)
            bytes_from_bot =struct.unpack(endian+'h', pg[4:6])[0]
            new_start_index = bytes_from_bot - len(tuple)
            pg[new_start_index:bytes_from_bot] = tuple
            pg[bytes_from_top:bytes_from_top+2] = struct.pack(endian+'h', new_start_index)
            pg[4:6] = struct.pack(endian+'h', new_start_index)
            pg[2:4] = struct.pack(endian+'h', number_tuples+1)
            assert(len(pg)==SIZE_OF_PAGE)
    else:
        assert(len(tuple)<pg_available_bytes(fbytes, pg_num))
        number_tuples = struct.unpack(endian+'h', pg[2:4])[0]
        bytes_from_top = 16+(2*number_tuples)
        bytes_from_bot =struct.unpack(endian+'h', pg[4:6])[0]
        new_start_index = bytes_from_bot - len(tuple)
        pg[new_start_index:bytes_from_bot] = tuple
        pg[bytes_from_top:bytes_from_top+2] = struct.pack(endian+'h', new_start_index)
        pg[4:6] = struct.pack(endian+'h', new_start_index)
        pg[2:4] = struct.pack(endian+'h', number_tuples+1)
        assert(len(pg)==SIZE_OF_PAGE)
    save_pg(fname, pg_num, pg)
    return None

def pg_delete_tuples_on_and_after(fname, pg_num, cell_ind):
    fbytes = load_file(fname)
    pg = load_pg(fbytes, pg_num)
    pg = bytearray(pg)
    number_tuples = struct.unpack(endian+'h', pg[2:4])[0]
    assert(cell_ind<=number_tuples-1)
    assert(number_tuples>=1)
    assert(cell_ind>=0)
    cell_content_area_start = struct.unpack(endian+'h', pg[4:6])[0]
    cell_top_loc, cell_bot_loc = get_tuple_indices(pg, cell_ind)
    dis = cell_bot_loc - cell_content_area_start
    pg[cell_content_area_start:cell_bot_loc] = b'\x00'*dis
    pg[16+2*cell_ind:16+2*number_tuples] = b'\x00'*2*(number_tuples-cell_ind)
    pg[2:4] = struct.pack(endian+'h', cell_ind)
    pg[4:6] = struct.pack(endian+'h', cell_content_area_start+dis)
    save_pg(fname, pg_num, pg)
    assert(len(pg)==SIZE_OF_PAGE)
    return (number_tuples - 1) == 0

def tab_interior_split_pg(fname, split_pg_num, cell2insert, new_rightmost_pg):
    pgs = read_all_pgs_in_file(fname)
    values = pgs[split_pg_num]

    tab_name = fname[:-4]
    parent_num = values['parent_pg']
    is_interior = not values['is_leaf']
    is_tab = values['is_tab']
    assert(is_tab)
    assert(is_interior)
    number_tuples = values['number_tuples']
    cells = values['cells']
    mid_tuple = int((number_tuples+1)//2)
    mid_tuple_binary = cells[mid_tuple]['cell_binary']
    mid_rowid = cells[mid_tuple]['rowid']
    rightmost_child_pg_right = new_rightmost_pg
    rightmost_child_pg_left = cells[mid_tuple]['left_child_pg']
    if parent_num==-1:
        right_child_num = write_new_pg(tab_name, is_tab, is_interior, new_rightmost_pg, split_pg_num)
        left_child_num = write_new_pg(tab_name, is_tab, is_interior, rightmost_child_pg_left, split_pg_num)
        copyoftuples=[]
        for i in range(mid_tuple):
            copyoftuples.append(cells[i]['cell_binary'])
            update_pg_header(fname, cells[i]['left_child_pg'], parent=left_child_num)
        update_pg_header(fname, rightmost_child_pg_left, parent=left_child_num)
        pg_insert_tuple(fname, left_child_num, copyoftuples)

        copyoftuples=[]
        for i in range(mid_tuple+1, number_tuples):
            copyoftuples.append(cells[i]['cell_binary'])
            update_pg_header(fname, cells[i]['left_child_pg'], parent=right_child_num)
        update_pg_header(fname, rightmost_child_pg_right, parent=right_child_num)
        pg_insert_tuple(fname, right_child_num, copyoftuples)
        pg_insert_tuple(fname, right_child_num, cell2insert)
        pg_delete_tuples_on_and_after(fname, split_pg_num, 0)
        pg_insert_tuple(fname, split_pg_num, mid_tuple_binary)
        update_tuple_leftpointer(fname, split_pg_num, 0, left_child_num)
        update_pg_header(fname, split_pg_num, right_sib_right_child=right_child_num)
        return right_child_num

    else:
        right_sib = write_new_pg(tab_name, is_tab, is_interior, rightmost_child_pg_right, parent_num)

        copyoftuples=[]
        for i in range(mid_tuple+1, number_tuples):
            copyoftuples.append(cells[i]['cell_binary'])
            update_pg_header(fname, cells[i]['left_child_pg'], parent=right_sib)
        update_pg_header(fname, rightmost_child_pg_right, parent=right_sib)

        pg_insert_tuple(fname, right_sib, copyoftuples)
        pg_insert_tuple(fname, right_sib, cell2insert)
        pg_delete_tuples_on_and_after(fname, split_pg_num, mid_tuple)

        mid_tuple_binary = tab_create_tuple([], [], True, left_child_pg=split_pg_num,  rowid=mid_rowid)
        update_pg_header(fname, split_pg_num, right_sib_right_child=rightmost_child_pg_left)

        if pgs[parent_num]['rightmost_child_pg']==split_pg_num:
            update_pg_header(fname, parent_num, right_sib_right_child=right_sib)
        try:
            pg_insert_tuple(fname, parent_num, mid_tuple_binary)
        except:
            new_parent = tab_interior_split_pg(fname, parent_num, mid_tuple_binary, right_sib)
            update_pg_header(fname, right_sib, parent = new_parent)
            update_pg_header(fname, split_pg_num, parent = new_parent)
        return right_sib

def tab_leaf_split_pg(fname, split_pg_num, cell2insert):
    fbytes = load_file(fname)
    values = read_tuples_in_pg(fbytes, split_pg_num)
    tab_name = fname[:-4]
    parent_num = values['parent_pg']
    is_interior = not values['is_leaf']
    is_leaf = values['is_leaf']
    is_tab = values['is_tab']
    assert(is_tab)
    assert(is_leaf)
    number_tuples = values['number_tuples']
    cells = values['cells']
    mid_tuple = int((number_tuples+1)/2)
    mid_tuple_binary = cells[mid_tuple]['cell_binary']
    mid_rowid = cells[mid_tuple]['rowid']
    right_sibling_pg = values['right_sibling_pg']
    if parent_num==-1:
        right_child_num = write_new_pg(tab_name, is_tab, False, -1, split_pg_num)
        left_child_num = write_new_pg(tab_name, is_tab, False, right_child_num, split_pg_num)
        copyoftuples = []
        for i in range(mid_tuple):
            copyoftuples.append(cells[i]['cell_binary'])
        pg_insert_tuple(fname, left_child_num, copyoftuples)
        copyoftuples = []
        for i in range(mid_tuple, number_tuples):
            copyoftuples.append(cells[i]['cell_binary'])
        pg_insert_tuple(fname, right_child_num, copyoftuples)
        pg_insert_tuple(fname, right_child_num, cell2insert)
        mid_tuple_binary = tab_create_tuple([], [], True, left_child_pg=left_child_num,  rowid=mid_rowid)
        pg_delete_tuples_on_and_after(fname, split_pg_num, 0)
        pg_insert_tuple(fname, split_pg_num, mid_tuple_binary)
        update_pg_header(fname, split_pg_num, right_sib_right_child=right_child_num, is_interior=True)
    else:
        right_sib = write_new_pg(tab_name, is_tab, is_interior, right_sibling_pg, parent_num)
        update_pg_header(fname, split_pg_num, right_sib_right_child=right_sib)
        copyoftuples = []
        for i in range(mid_tuple, number_tuples):
            copyoftuples.append(cells[i]['cell_binary'])
        pg_insert_tuple(fname, right_sib, copyoftuples)
        pg_insert_tuple(fname, right_sib, cell2insert)
        pg_delete_tuples_on_and_after(fname, split_pg_num, mid_tuple)
        update_pg_header(fname, parent_num, right_sib_right_child=right_sib)
        mid_tuple_binary = tab_create_tuple([], [], True, left_child_pg=split_pg_num,  rowid=mid_rowid)
        try:
            pg_insert_tuple(fname, parent_num, mid_tuple_binary)
        except:
            new_parent = tab_interior_split_pg(fname, parent_num, mid_tuple_binary, right_sib)
            update_pg_header(fname,right_sib, parent = new_parent)
            update_pg_header(fname, split_pg_num, parent = new_parent)

def tab_insert(tab_name, values):
    schema, all_col_data = catalog_schema(tab_name)
    next_pg, next_rowid = get_next_pg_rowid(tab_name)
    tuple = tab_create_tuple(schema, values, False,  rowid=next_rowid)
    try:
        pg_insert_tuple(tab_name+'.tbl', next_pg, tuple)
    except:
        tab_leaf_split_pg(tab_name+'.tbl', next_pg, tuple)
    return None

def pg_tuple_ind_given_key(pgs, index_value):
    pg_num=0
    if len(pgs[pg_num]['cells'])==0:
        return pg_num, 0
    return get_pg_tuple_ind(pgs, index_value, pg_num)

def get_pg_tuple_ind(pgs, value, pg_num):
    pg = pgs[pg_num]
    is_tab= pg['is_tab']
    is_leaf = pg['is_leaf']
    if not is_tab:
        for i, tuple in enumerate(pg['cells']):
            if tuple['index_value']==value:
                return pg_num, i
            elif tuple['index_value'] > value:
                if not pg['is_leaf']:
                    return get_pg_tuple_ind(pgs, value, tuple['left_child_pg'])
                else:
                    return pg_num, i
            else:
                if pg['is_leaf'] and i+1==len(pg['cells']):
                    return pg_num, len(pg['cells'])
                if not pg['is_leaf'] and i+1==len(pg['cells']):
                    return get_pg_tuple_ind(pgs, value, pg['rightmost_child_pg'])
                else:
                    continue
    else:
        for cell_ind, tuple in enumerate(pg['cells']):
            if (tuple['rowid'] == value):
                if is_leaf:
                    return pg_num, cell_ind
                else:
                    if cell_ind+1==len(pg['cells']):
                        return get_pg_tuple_ind(pgs, value, pg['rightmost_child_pg'])
                    else:
                        continue
            elif (tuple['rowid'] > value):
                if not is_leaf:
                    return get_pg_tuple_ind(pgs, value, tuple['left_child_pg'])
                else:
                    return pg_num, cell_ind
            elif cell_ind+1==len(pg['cells']):
                if not is_leaf:
                    return get_pg_tuple_ind(pgs, value, pg['rightmost_child_pg'])
                else:
                    return pg_num, len(pg['cells'])
            else:
                continue
def tab_delete(fname, rowids):
    pgs = read_all_pgs_in_file(fname)
    for rowid in rowids:
        pg_num=0
        tab_delete_recursion(pgs, pg_num, rowid)
    pg_dict_to_file(fname, pgs)

def delete_from_pg_dict(pgs, pg_num, ind):
    pg = pgs[pg_num]
    pg['available_bytes']+=len(pg['cells'][ind]['cell_binary'])
    pg['number_tuples']-=1
    del pg['rowids'][ind]
    del pg['cells'][ind]
    return pgs

def insert_to_pg_dict(pgs, pg_num, tuple, ind):
    pg = pgs[pg_num]
    pg['available_bytes']-=len(tuple['cell_binary'])
    pg['number_tuples']+=1
    if ind==0:
        pg['rowids'] = [tuple['rowid']] + pg['rowids']
        pg['cells'] = [tuple] + pg['cells']
    elif ind+1<pg['number_tuples']:
        pg['rowids'] = pg['rowids'][:ind] + [tuple['rowid']] + pg['rowids'][ind:]
        pg['cells'] = pg['cells'][:ind] + [tuple] + pg['cells'][ind:]
    else:
        pg['rowids'].append(tuple['rowid'])
        pg['cells'].append(tuple)
    return pgs

def update_tuple_bin(cell_binary, rowid=None, left_child=None):
    cell_binary = bytearray(cell_binary)
    if left_child!=None:
        cell_binary[:4] = struct.pack(endian+'i', left_child)
    if rowid!=None:
        cell_binary[4:8] = struct.pack(endian+'i', rowid)
    return cell_binary

def fix_parent_pointer(pgs, parent_pg, id2fix, left=True):
    pg = pgs[parent_pg]
    for i, id in enumerate(pg['rowids']):
        if left:
            if id > id2fix:
                pg['rowids'][i]=id2fix-1
                pg['cells'][i]['cell_binary']=update_tuple_bin(pg['cells'][i]['cell_binary'], rowid=id2fix-1)
                pg['cells'][i]['rowid']=id2fix-1
                break
        else:
            if id > id2fix:
                pg['rowids'][i-1]=id2fix
                pg['cells'][i-1]['cell_binary']=update_tuple_bin(pg['cells'][i-1]['cell_binary'], rowid=id2fix)
                pg['cells'][i-1]['rowid']=id2fix
                break
    return None

def steal_sibling_tuple(pgs, pg_num, left=True):
    pg = pgs[pg_num]
    if left:
        left_sib = pgs[pg['left_sibling_pg']]
        tuple = left_sib['cells'][-1]
        insert_to_pg_dict(pgs, pg_num, tuple, 0)
        delete_from_pg_dict(pgs, left_sib['pg_number'], left_sib['number_tuples']-1)
        return tuple['rowid']+1
    else:
        rsib = pgs[pg['right_sibling_pg']]
        tuple = rsib['cells'][0]
        insert_to_pg_dict(pgs, pg_num, tuple, pg['number_tuples'])
        delete_from_pg_dict(pgs, rsib['pg_number'], 0)
        return tuple['rowid']+1

def try_borrowing(pgs, borrower_pg):
    pg = pgs[borrower_pg]
    parent = pg['parent_pg']
    if 'left_sibling_pg' in pg:
        left_sib = pgs[pg['left_sibling_pg']]
        steal_size = len(left_sib['cells'][-1]['cell_binary'])
        if left_sib['number_tuples']>2:
            id2fix=steal_sibling_tuple(pgs, borrower_pg, left=True)
            fix_parent_pointer(pgs, pg['parent_pg'], id2fix, left=True)
            return None
        else:
            return 'left'
    elif pg['right_sibling_pg']!=-1 and pgs[pg['right_sibling_pg']]['parent_pg']==parent:
        rsib = pgs[pg['right_sibling_pg']]
        steal_size = len(rsib['cells'][0]['cell_binary'])
        if rsib['number_tuples']>2:
            id2fix=steal_sibling_tuple(pgs, borrower_pg, left=False)
            fix_parent_pointer(pgs, pg['parent_pg'], id2fix, left=False)
            return None
        else:
            return 'right'
    else:
        return None

def delete_dict(pgs, pg_num, rowid):
    pg = pgs[pg_num]
    if rowid in pg['rowids']:
        ind = pg['rowids'].index(rowid)
        pgs = delete_from_pg_dict(pgs, pg_num, ind)
        if pg['number_tuples']<2 and pg['parent_pg']!=-1:
            return try_borrowing(pgs, pg_num)
        else:
            return None
    else:
        return None

def merge_children(pgs, pg_num, child_pg_num, left=True):
    pg = pgs[pg_num]
    pg_children = [i['left_child_pg'] for i in pgs[pg_num]['cells']]
    child_pg = pgs[child_pg_num]
    if left:
        left_sib = pgs[child_pg['left_sibling_pg']]
        id2del = pg['cells'][pg_children.index(left_sib['pg_number'])]['rowid']
        if not left_sib['is_leaf']:
            for tuple in left_sib['cells']:
                pgs[tuple['left_child_pg']]['parent_pg'] = child_pg_num
            pgs[left_sib['rightmost_child_pg']]['parent_pg'] = child_pg_num
            mid_tuple = tab_create_tuple([], [], True, left_child_pg=left_sib['rightmost_child_pg'],  rowid=id2del)
            left_sib['cells'] = left_sib['cells']+mid_tuple
            left_sib['rowids'] = left_sib['rowids']+id2del
        child_pg['cells'] = left_sib['cells']+child_pg['cells']
        child_pg['rowids'] = left_sib['rowids']+child_pg['rowids']
        child_pg['number_tuples'] = left_sib['number_tuples']+child_pg['number_tuples']
        child_pg['available_bytes'] = left_sib['available_bytes']-(SIZE_OF_PAGE - child_pg['available_bytes'])
        pgs[child_pg['right_sibling_pg']]['left_sibling_pg'] = child_pg_num
        if 'left_child_pg' not in left_sib:
            del child_pg['left_sibling_pg']
        else:
            child_pg['left_child_pg'] = left_sib['left_child_pg']
        delete_pg_in_dictionary(pgs, left_sib['pg_number'])
        return id2del
    else:
        rsib = pgs[child_pg['right_sibling_pg']]
        id2del = pg['cells'][pg_children.index(child_pg_num)]['rowid']
        if not rsib['is_leaf']:
            for tuple in rsib['cells']:
                pgs[tuple['left_child_pg']]['parent_pg'] = child_pg_num
            pgs[rsib['rightmost_child_pg']]['parent_pg'] = child_pg_num
            mid_tuple = tab_create_tuple([], [], True, left_child_pg=rsib['rightmost_child_pg'],  rowid=id2del)
            child_pg['cells'] = child_pg['cells']+mid_tuple
            child_pg['rowids'] = child_pg['rowids']+id2del
        child_pg['cells'] = child_pg['cells'] + rsib['cells']
        child_pg['rowids'] = child_pg['rowids'] + rsib['rowids']
        child_pg['number_tuples'] = child_pg['number_tuples'] + rsib['number_tuples']
        child_pg['available_bytes'] = rsib['available_bytes']-(SIZE_OF_PAGE - child_pg['available_bytes'])
        if ['right_sibling_pg']!=-1 and 'left_sibling_pg' in child_pg:
            pgs[child_pg['left_sibling_pg']]['right_sibling_pg'] = child_pg_num
        child_pg['right_sibling_pg'] = rsib['right_sibling_pg']
        i = pg_children.index(rsib['pg_number'])
        tuple = pg['cells'][i]
        if i+1 ==len(pgs[pg['parent_pg']]['cells']):
            pgs[pg['parent_pg']]['rightmost_child_pg'] = child_pg['pg_number']
        else:
            tuple['left_child_pg'] = child_pg['pg_number']
            tuple['cell_binary'] = update_tuple_bin(tuple['cell_binary'], left_child=child_pg['pg_number'])
        delete_pg_in_dictionary(pgs, rsib['pg_number'])
        return id2del

def tab_delete_recursion(pgs, pg_num, rowid):
    pg = pgs[pg_num]
    if pg['is_leaf']:
        return delete_dict(pgs, pg_num, rowid)
    else:
        for  i, tuple in enumerate(pg['cells']):
            if tuple['rowid'] > rowid:
                child_pg = tuple['left_child_pg']
                merge_child = tab_delete_recursion(pgs, child_pg, rowid)
                break
            elif i+1 == len(pg['cells']):
                child_pg = pg['rightmost_child_pg']
                merge_child = tab_delete_recursion(pgs, child_pg, rowid)
                break
            else:
                continue

        if merge_child is None:
            return None
        elif merge_child=='left':
            if pg['parent_pg']==-1 and pg['number_tuples']==1:
                merge_children(pgs, pg_num, child_pg, left=True)
                pgs[pg['rightmost_child_pg']]['parent_pg'] = -1
                delete_pg_in_dictionary(pgs, pg_num)
            else:
                id2del = merge_children(pgs, pg_num, child_pg, left=True)
                return delete_dict(pgs, pg_num, id2del)

        elif merge_child=='right':
            if pg['parent_pg']==-1 and pg['number_tuples']==1:
                merge_children(pgs, pg_num, child_pg, left=False)
                pgs[pg['cells'][0]['left_child_pg']]['parent_pg'] = -1
                delete_pg_in_dictionary(pgs, pg_num)
            else:
                id2del = merge_children(pgs, pg_num, child_pg, left=False)
                return delete_dict(pgs, pg_num, id2del)
        else:
            assert(False)

def copy_pg(fname, pgs, pg_number, parent, i=None):
    pg = pgs[pg_number]
    tab_name = fname[:-4]
    is_tab = pg['is_tab']
    is_interior = not pg['is_leaf']
    if is_interior:
        right_sib_right_child = pg['rightmost_child_pg']
    else:
        right_sib_right_child = pg['right_sibling_pg']
    if i==None:
        i=0
    write_new_pg(tab_name, is_tab, is_interior, right_sib_right_child, parent)
    cells = pg['cells']
    if is_interior:
        if cells!=sorted(cells, key=lambda x: x['left_child_pg']):
            for tuple, scell in zip(cells, sorted(cells, key=lambda x: x['left_child_pg'])):
                tuple['cell_binary'] = update_tuple_bin(tuple['cell_binary'], left_child=scell['left_child_pg'])
    if pg['number_tuples']>1:
        cells2insert = [j["cell_binary"] for j in cells]
    else:
        cells2insert = cells[0]['cell_binary']
    pg_insert_tuple(fname, i, cells2insert)
    if is_interior:
        children = [j["left_child_pg"] for j in cells]+[pg['rightmost_child_pg']]
        for child in children:
            if 'deleted' not in pgs[child]:
                i+=1
                if children.index(child)+1==len(children):
                    update_pg_header(fname, pg_number, right_sib_right_child=i, is_interior=is_interior, parent=parent)
                i = copy_pg(fname, pgs, child, pg_number, i=i)
            else:
                continue
    return i

def delete_pg_in_dictionary(pgs, pg_number):
    del pgs[pg_number]
    for pg in pgs:
        if pg['pg_number']>=pg_number:
            pg['pg_number']-=1
        if pg['parent_pg']!=-1:
            if 'left_sibling_pg' in pg:
                if pg['left_sibling_pg']>=pg_number:
                    pg['left_sibling_pg']-=1
            if pg['right_sibling_pg']!=-1 and 'right_sibling_pg' in pg:
                if pg['right_sibling_pg']>=pg_number:
                    pg['right_sibling_pg']-=1
        if pg['parent_pg']>=pg_number:
            pg['parent_pg']-=1
        if not pg['is_leaf']:
            if pg['rightmost_child_pg']>=pg_number:
                pg['rightmost_child_pg']-=1
            for tuple in pg['cells']:
                if tuple['left_child_pg']>=pg_number:
                    tuple['left_child_pg']-=1
                    tuple['cell_binary'] = update_tuple_bin(tuple['cell_binary'], left_child=tuple['left_child_pg'])

def pg_dict_to_file(fname, pgs):
    tab_name = fname[:-4]
    if fname[-4:]=='.tbl':
        is_tab = True
    else:
        is_tab=False
    for pg in pgs:
        if pg['parent_pg']==-1:
            root_node = pg['pg_number']
            break

    os.remove(fname)
    with open(fname, 'w+') as f:
        pass
    copy_pg(fname, pgs, root_node, -1)
    return None

def datatype_to_python(datatype):
    datatype = datatype.lower()
    mapping = {"null":None,"tinyint":int, "smallint":int, "int":int, "bigint":int, "long":int, 'float':float, "double":float, "year":int, "time":datetime, "datetime":datetime, "date":datetime, "text":str}
    return mapping[datatype]

def to_python(schema_columns, schema, column, v):
    i = schema_columns.index(column)
    py = datatype_to_python(schema[i])
    if v=='NULL':
        return None
    if py != None:
        if schema[i].lower()=='datetime':
            try:
                return datetime.strptime(v, '%m/%d/%Y %H:%M:%S')
            except:
                return datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
        elif schema[i].lower()=='date':
            try:
                return datetime.strptime(v, '%m/%d/%Y')
            except:
                return datetime.strptime(v, '%Y-%m-%d')
        elif schema[i].lower()=='time':
            try:
                return datetime.strptime("1/2/1970 "+v, '%m/%d/%Y %H:%M:%S')
            except:
                return datetime.strptime("1/2/1970 "+v, '%m/%d/%Y %I:%M%p')
        else:
            return py(v)
    else:
        return None

def extract_definitions(token_list):
    definitions = []
    tmp = []
    tidx, token = token_list.token_next(1)
    while token and not token.match(sqlparse.tokens.Punctuation, ')'):
        tmp.append(token)
        tidx, token = token_list.token_next(tidx, skip_ws=False)
        if token and token.match(sqlparse.tokens.Punctuation, ','):
            definitions.append(tmp)
            tmp = []
            tidx, token = token_list.token_next(tidx)
    if tmp and isinstance(tmp[0], sqlparse.sql.Identifier):
        definitions.append(tmp)
    return definitions

def parse_create_tab(SQL):
    SQL = SQL.rstrip()
    parsed = sqlparse.parse(SQL)[0]
    tab_name = str(parsed[4])
    _, par = parsed.token_next_by(i=sqlparse.sql.Parenthesis)
    columns = extract_definitions(par)
    col_list = []
    definition_list = []
    for column in columns:
        definitions = ''.join(str(t) for t in column).split(',')
        for definition in definitions:
            d = ' '.join(str(t) for t in definition.split())
            col_list.append(definition.split()[0])
            definition_list.append(d)

    d = {}
    d[tab_name] = {}
    c = 1
    for col, definition in zip(col_list, definition_list):
        isnull = 'NO'
        isunique = 'NO'
        isprimary = 'NO'
        definition = definition[len(col)+1:]
        if 'NOT NULL' in definition:
            isnull = 'YES'
        elif 'UNIQUE' in definition:
            isunique = 'YES'
        elif 'PRIMARY KEY' in definition:
            isprimary = 'YES'
            isunique = 'YES'
            isnull = 'YES'
        d[tab_name][col] = {"data_type" : definition.split()[0],
                              "ordinal_position" : c,
                               'is_nullable':isnull,
                                'unique':isunique,
                                'primary_key':isprimary}
    return d

def parse_insert_into(cmd_input):
    query_match = "(?i)insert (?i)into\s+(.*?)\s*((?i)values\s(.*?)\s*)?;"
    if re.match(query_match, cmd_input):
        stmt = sqlparse.parse(cmd_input)[0]
        tab_name = str(stmt.tokens[4])
        col_names = tab_name[tab_name.find("(")+1:tab_name.find(")")]
        column_list = [x.strip() for x in col_names.split(',')]
        tab_name = tab_name.split()[0]
        values = str(stmt.tokens[-2])
        values = re.sub("\s", "", re.split(';',re.sub("(?i)values","",values))[0])
        vals= values.replace(' ', '')
        values = []
        for v in vals.split('('):
            if len(v)==0:
                continue
            v = v.replace('),','')
            v = v.replace(',)','')
            v = v.replace(')','')
            values.append(v.split(','))

        schema_col_names = get_col_names_from_catalog(tab_name)[1:]
        schema, _ = catalog_schema(tab_name)
        vals = []
        for value in values:
            temp = []
            for col in schema_col_names:
                if col.upper() in column_list:
                    j = column_list.index(col.upper())
                    v = value[j]
                    temp.append(to_python(schema_col_names, schema, col, v))
                else:
                    temp.append(None)
            vals.append(temp)
        return tab_name, vals
    else:
        print("Please enter correct query")
        return None

def parse_drop_tab(cmd_input):
    return cmd_input[len("drop table "):-1].lower()
    query_match = "(?i)DROP\s+(.*?)\s*(?i)TABLE\s+[a-zA-Z]+\;"
    if re.match(query_match, cmd_input):
        stmt = sqlparse.parse(cmd_input)[0]
        tabname = str(stmt.tokens[-2])
        return tabname
    else:
        print("Please enter correct query")

def create_index(cmd_input):
    print("create index \'{}\'".format(cmd_input))
    return None

def get_operator_fn(op):
    return {
    '=' : operator.eq,
    '<' : operator.lt,
    '>' : operator.gt,
    '>=' : operator.ge,
    '<=' : operator.le,
    }[op]

def query(cmd_input: str):
    operator_list = ['=','>','<','>=','<=']
    query_match = "select\s+(.*?)\s*(?i)from\s+(.*?)\s*((?i)where\s(.*?)\s*)?;"
    if "WHERE" not in cmd_input:
        cmd_input = cmd_input[:-1]+" WHERE ROWID > 0;"
    stmt = sqlparse.parse(cmd_input)[0]
    where_clause = str(stmt.tokens[-1])
    where_clause = re.sub("\s", "", re.split(';',re.sub("(?i)where","",where_clause))[0])
    res = [i for i in operator_list if where_clause.find(i)!=-1]
    where_clause = re.split('>=|<=|=|>|<|\s',where_clause)
    tabname = str(stmt.tokens[-3]).split(",")[0]
    columns = str(stmt.tokens[2]).split(",")
    return str(where_clause[0]),str(where_clause[1]),res[-1], tabname, columns

def where(SQL):
    operand_where, value_where, oper, tab_name, columns =  query(SQL)
    tab_name=tab_name.lower()
    schema, _ = catalog_schema(tab_name)
    if not os.path.exists(tab_name.lower()+'.tbl'):
        print("Table {} does not exist.".format(tab_name))
        return None, None
    column_list = get_col_names_from_catalog(tab_name)
    index = column_list.index(operand_where.lower())
    if operand_where == -1:
        print("Please enter correct query")
    matched_tuples = []
    flag = False
    for node in read_all_pgs_in_file(tab_name + ".tbl"):
        if node['is_leaf'] :
            for tuple in node['cells']:
                data = tuple['data']
                if index == 0 :
                    operand1 = tuple['rowid']
                    operand2 = int(value_where)
                else:
                    operand2 = to_python(column_list[1:], schema, operand_where.lower(), value_where)
                    operand1 = data[index - 1]
                if get_operator_fn(oper)(operand1, operand2):
                    matched_tuples.append(tuple)
    return tab_name, matched_tuples

def validate(fname, pgs=None, pg_num=0, is_tab=None):
    if pg_num==0:
        pgs =read_all_pgs_in_file(fname)
        is_tab = pgs[0]['is_tab']
    pg = pgs[pg_num]
    if pg['parent_pg']!=-1:
        parent_children = [i['left_child_pg'] for i in pgs[pg['parent_pg']]['cells']]
        parent_children.append(pgs[pg['parent_pg']]['rightmost_child_pg'])
        try:
            assert(pg_num in parent_children)
        except:
            print("parent", pgs[pg['parent_pg']], "does not point to ", pg_num)
            assert(False)

    if  not pg['is_leaf']:
        if not is_tab:
            key = 'index_value'
        else:
            key='rowid'
        for i, tuple in enumerate(pg['cells']):
            if i==0:
                continue
            else:
                c_max = max([i[key] for i in pgs[tuple['left_child_pg']]['cells']])
                c_min = min([i[key] for i in pgs[tuple['left_child_pg']]['cells']])
                try:
                    assert((pg['cells'][i-1][key]<=c_min) and (c_max<tuple[key]))
                except:
                    print("pg_num incorrect ordering", pg_num, 'child',tuple['left_child_pg'])
                    assert(False)

    if not pg['is_leaf']:
        for tuple in pg['cells']:
            try:
                validate(fname, pgs=pgs, pg_num=tuple['left_child_pg'], is_tab=is_tab)
            except:
                assert(False)
        try:
            validate(fname, pgs=pgs, pg_num=pg['rightmost_child_pg'], is_tab=is_tab)
        except:
                assert(False)
    else:
        return

def tuple_print(tab_name, cells):
    schema, _ = catalog_schema(tab_name)
    columns = get_col_names_from_catalog(tab_name)
    str_f1 = '{:^12}|'
    for s in schema:
        if s.lower()=='text':
            str_f1 += '{:^25}|'
        else:
            str_f1 += '{:^12}|'
    str_f1 = str_f1[:-1]
    print(str_f1.format(*columns))
    cells = sorted(cells, key=lambda x: x['rowid'])
    for tuple in cells:
        data =[]
        for d, st in zip(tuple['data'], schema):
            if d==None:
                data.append('NULL')
            elif st.lower()=='date':
                data.append(str(d.date()))
            elif st.lower()=='time':
                data.append(str(d))
            elif st.lower()=='datetime':
                data.append(str(d))
            elif st.lower()=='float':
                data.append(round(d,4))
            elif st.lower()=='double':
                data.append(round(d,4))
            else:
                data.append(d)
        print(str_f1.format(tuple['rowid'], *data))
    return None

if __name__== "__main__":
    init()
    print("DavisBase : version 1.0")
    print("Enter \"help;\" for Command usage.")
    exit_cmd_input = False
    cmd_input = ''
    while not exit_cmd_input:
        line = input("davisql> ").upper()
        if len(cmd_input)==0:
            cmd_input+=line.rstrip()
        else:
            cmd_input+=" "+line.rstrip()
        output = read_input(cmd_input)
        if type(output)==bool:
            exit_cmd_input = True
        elif output=='break':
            pdb.set_trace()
            break
        elif output==None:
            cmd_input=''
        else:
            continue
