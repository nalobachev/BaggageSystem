import pandas as pd
import numpy as np
import simpy
from simpy.util import start_delayed
import arrow
import networkx as nx

new_data = pd.read_csv("result2019.csv", index_col=0)
transit = new_data[new_data['isTransit'] == 1]
transit = transit.sort_values(by='ArrDateTime')
transit = transit.drop_duplicates(subset='TagID', keep='last')
index_f = transit[(transit['ArrTerminal'] == 'F') | (transit['DepartureTerminal'] == 'F')].index
transit.drop(index_f , inplace=True)

data = pd.read_excel("Data4SVO (1).xlsx")
data_ctt = pd.read_excel("Data4SVO (1).xlsx", sheet_name=1)

class Terminal(object):
    def __init__(self, env, TerminalID, BHS_time, INBP_time, OUTBP_time, TBS_time, LTTBS_time,
                 BHS_capacity, INBP_capacity, OUTBP_capacity, TBS_capacity, LTTBS_capacity):
        self.env = env
        self.TerminalID = TerminalID

        self.BHS_time = BHS_time
        self.INBP_time = INBP_time
        self.OUTBP_time = OUTBP_time
        self.TBS_time = TBS_time
        self.LTTBS_time = LTTBS_time

        self.BHS_capacity = BHS_capacity
        self.INBP_capacity = INBP_capacity
        self.OUTBP_capacity = OUTBP_capacity
        self.TBS_capacity = TBS_capacity
        self.LTTBS_capacity = LTTBS_capacity

        self.BHS = simpy.Resource(env, BHS_capacity)
        self.INBP = simpy.Resource(env, INBP_capacity)
        self.OUTBP = simpy.Resource(env, OUTBP_capacity)
        self.TBS = simpy.Resource(env, TBS_capacity)
        self.LTTBS = simpy.Resource(env, LTTBS_capacity)

        self.BHS_transfer = simpy.Resource(env, 1)
        self.INBP_transfer = simpy.Resource(env, 1)
        self.OUTBP_transfer = simpy.Resource(env, 1)
        self.TBS_transfer = simpy.Resource(env, 1)
        self.LTTBS_transfer = simpy.Resource(env, 1)

        self.BHS_line = simpy.Resource(env, 10000000)
        self.INBP_line = simpy.Resource(env, 10000000)
        self.OUTBP_line = simpy.Resource(env, 10000000)
        self.TBS_line = simpy.Resource(env, 10000000)
        self.LTTBS_line = simpy.Resource(env, 10000000)

        self.INBP_line_full = False
        self.BHS_line_full = False
        self.OUTBP_line_full = False
        self.TBS_line_full = False
        self.LTTBS_line_full = False

        self.BHS_storage_full = False
        self.INBP_storage_full = False
        self.OUTBP_storage_full = False
        self.TBS_storage_full = False
        self.LTTBS_storage_full = False

class CTT(object):
    def __init__(self, env, label, ProcessingTime, QueryCapacity, Capacity):
        self.env = env
        self.label = label
        self.ProcessingTime = ProcessingTime
        self.QueryCapacity = QueryCapacity
        self.Capacity = Capacity

        self.CTT_resource = simpy.Resource(env, Capacity)
        self.CTT_line = simpy.Resource(env, 10000000)
        self.CTT_transfer = simpy.Resource(env, 1)

        self.CTT_line_full = False
        self.CTT_resource_full = False

class Station(object):
    def __init__(self, env, ProcessingTime, QueryCapacity, Capacity, label):
        self.env = env
        self.ProcessingTime = ProcessingTime
        self.QueryCapacity = QueryCapacity
        self.Capacity = Capacity
        self.label = label

        self.station_resource = simpy.Resource(env, Capacity)
        self.station_line = simpy.Resource(env, 10000000)
        self.station_transfer = simpy.Resource(env, 1)

        self.station_line_full = False
        self.station_resource_full = False

start = arrow.get(transit.iloc[0]['ArrDateTime'], 'DD.MM.YY HH:mm:ss')
env = simpy.Environment(initial_time=start.timestamp())

def add_to_log(eventTypeID, TagID, TerminalID, SystemID, FlNum):
    DateTime = arrow.get(env.now).format('YYYY-MM-DD HH:mm:ss')
    row = {'DateTime':DateTime, 'eventTypeID': eventTypeID, 
           'TagID':TagID, 'TerminalID': TerminalID, 'SystemID': SystemID, 'FlNum':FlNum}
    log.append(row)

terminal_labels = ['B', 'C', 'D', 'E',]
terminals = {}
for terminal in terminal_labels:
    term_data = data[data['TerminalID  '] == terminal]
    times = term_data['ProcessingTime  '].tolist()
    capacities = term_data['Capacity'].tolist()
    temp = Terminal(env, terminal, times[0], times[1], times[2], times[3], times[4],
                    capacities[0], capacities[1], capacities[2], capacities[3], capacities[4])
    terminals.update({terminal:temp})

ctt_1 = CTT(env, 1, 420, 20, 100)
ctt_2 = CTT(env, 2, 420, 20, 100)

south_station = Station(env, 120, 20, 100, 'South Station')
north_station = Station(env, 120, 20, 100, 'North Station')
stations = {'south':south_station, 'north':north_station}

terminals_in_south = ['D', 'E']
terminals_in_north = ['C', 'B']

def check_stations(south_station, north_station, terminal):
    if terminal in south_station:
        st_label = 'south' 
    else:
        st_label = 'north'
    return st_label

env_resources = []
env_resources_labels = []
for name in terminals:
    current_res = terminals[name]
    env_resources.append(current_res.BHS)
    env_resources.append(current_res.INBP)
    env_resources.append(current_res.OUTBP)
    env_resources.append(current_res.TBS)
    env_resources.append(current_res.LTTBS)

    env_resources_labels.append("%s_BHS" %name)
    env_resources_labels.append("%s_INBP" %name)
    env_resources_labels.append("%s_OUTBP" %name)
    env_resources_labels.append("%s_TBS" %name)
    env_resources_labels.append("%s_LTTBS" %name)

    env_resources.append(current_res.BHS_line)
    env_resources.append(current_res.INBP_line)
    env_resources.append(current_res.OUTBP_line)
    env_resources.append(current_res.TBS_line)
    env_resources.append(current_res.LTTBS_line)

    env_resources_labels.append("%s_BHS_line" %name)
    env_resources_labels.append("%s_INBP_line" %name)
    env_resources_labels.append("%s_OUTBP_line" %name)
    env_resources_labels.append("%s_TBS_line" %name)
    env_resources_labels.append("%s_LTTBS_line" %name)

env_resources.append(ctt_1.CTT_resource)
env_resources.append(ctt_2.CTT_resource)
env_resources_labels.append("CTT_1")
env_resources_labels.append("CTT_2")

env_resources.append(ctt_1.CTT_line)
env_resources.append(ctt_2.CTT_line)
env_resources_labels.append("CTT_1_line")
env_resources_labels.append("CTT_2_line")

env_resources.append(south_station.station_resource)
env_resources.append(north_station.station_resource)
env_resources_labels.append("South Station")
env_resources_labels.append("North Station")

env_resources.append(south_station.station_line)
env_resources.append(north_station.station_line)
env_resources_labels.append("South Station Line")
env_resources_labels.append("North Station Line")

def add_to_system_log(resources, labels):
    DateTime = arrow.get(env.now).format('YYYY-MM-DD HH:mm:ss')
    row = {'DateTime':DateTime}
    for i, res in enumerate(resources):
        row[labels[i]] = res.count
    system_log.append(row)

plane_landed = set()
plane_in_air = set()
log = []
system_log = []
Time_to_move = 7200
A_lot_of_time = 14400
time_to_restart = 5

def plane_land(env, ArrDateTime, TagID, ArrTerminal, ArrFlNum, DepartureTerminal, DepFlNum, DepDateTime):
    global plane_landed
    global log
    global mon_data
    plane = (ArrFlNum, ArrDateTime)
    if plane not in plane_landed:
        plane_landed.add(plane)
        add_to_log("A001", "0", ArrTerminal, "NA", ArrFlNum)
        add_to_system_log(env_resources, env_resources_labels)
    env.process(baggage_move(env, ArrDateTime, TagID, ArrTerminal, ArrFlNum, DepartureTerminal, DepFlNum, DepDateTime))
    env.process(plane_takeoff(env, DepartureTerminal, DepDateTime, DepFlNum))
    yield env.timeout(0)

def plane_takeoff(env, DepartureTerminal, DepDateTime, DepFlNum):
    global log
    global plane_in_air 
    global mon_data
    plane = (DepFlNum, DepDateTime)
    if plane not in plane_in_air:
        plane_in_air.add(plane)
        wait = arrow.get(DepDateTime, 'DD.MM.YY HH:mm:ss').timestamp() -  arrow.get(env.now).timestamp()
        if wait >= 0:
            yield env.timeout(wait)
            add_to_log("A002", "0", DepartureTerminal, "NA", DepFlNum)
            add_to_system_log(env_resources, env_resources_labels)
    yield env.timeout(0)

def baggage_move(env, ArrDateTime, TagID, ArrTerminal, ArrFlNum, DepartureTerminal, DepFlNum, DepDateTime):
    global terminals
    # global airport
    global log
    # global ctts
    global Time_to_move
    global time_to_restart
    global stations
    global mon_data
    storage_flag = False

    current_terminal = terminals[ArrTerminal]
    terminal = ArrTerminal

    if current_terminal.INBP_line.count > 40 and not current_terminal.INBP_line_full:
        add_to_log("M001", TagID, terminal, "INBP", "NA")
        add_to_system_log(env_resources, env_resources_labels)
        current_terminal.INBP_line_full = True
    INBP_line_request = current_terminal.INBP_line.request()
    yield INBP_line_request
    add_to_log("B002", TagID, terminal, "INBP", "NA")
    add_to_system_log(env_resources, env_resources_labels)

    #try enter INBP
    INBP_storage_request = current_terminal.INBP.request()
    if len(current_terminal.INBP.queue) > 0 and not current_terminal.INBP_storage_full:
        add_to_log("M003", TagID, terminal, "INBP", "NA")
        add_to_system_log(env_resources, env_resources_labels)
        current_terminal.INBP_storage_full = True
    yield INBP_storage_request

    INBP_transfer_request = current_terminal.INBP_transfer.request()
    yield INBP_transfer_request
    current_terminal.INBP_line.release(INBP_line_request)
    yield env.timeout(time_to_restart)
    current_terminal.INBP_transfer.release(INBP_transfer_request)

    add_to_log("B001", TagID, terminal, "INBP", "NA")
    add_to_system_log(env_resources, env_resources_labels)
    if current_terminal.INBP_line.count <= 40 and current_terminal.INBP_line_full:
        add_to_log("M002", TagID, terminal, 'INBP', "NA")
        add_to_system_log(env_resources, env_resources_labels)
        current_terminal.INBP_line_full = False
    yield env.timeout(current_terminal.INBP_time)

    #try enter BHS line
    BHS_line_request = current_terminal.BHS_line.request()
    if current_terminal.BHS_line.count > 40 and not current_terminal.BHS_line_full:
        add_to_log("M001", TagID, terminal, 'BHS', "NA")
        add_to_system_log(env_resources, env_resources_labels)
        current_terminal.BHS_line_full = True
    yield BHS_line_request
    current_terminal.INBP.release(INBP_storage_request)
    if len(current_terminal.INBP.queue) == 0 and current_terminal.INBP_storage_full:
        add_to_log("M004", TagID, terminal, "INBP", "NA")
        add_to_system_log(env_resources, env_resources_labels)
        current_terminal.INBP_storage_full = False
    add_to_log("B002", TagID, terminal, 'BHS', "NA")
    add_to_system_log(env_resources, env_resources_labels)

    #try enter BHS
    BHS_storage_request = current_terminal.BHS.request()
    if len(current_terminal.BHS.queue) > 0 and not current_terminal.BHS_storage_full:
        add_to_log("M003", TagID, terminal, "BHS", "NA")
        add_to_system_log(env_resources, env_resources_labels)
        current_terminal.BHS_storage_full = True
    yield BHS_storage_request

    BHS_transfer_request = current_terminal.BHS_transfer.request()
    yield BHS_transfer_request
    current_terminal.BHS_line.release(BHS_line_request)
    yield env.timeout(time_to_restart)
    current_terminal.BHS_transfer.release(BHS_transfer_request)

    add_to_log('B001', TagID, terminal, 'BHS', "NA")
    add_to_system_log(env_resources, env_resources_labels)
    if current_terminal.BHS_line.count <= 40 and current_terminal.BHS_line_full:
        add_to_log("M002", TagID, terminal, 'BHS', "NA")
        add_to_system_log(env_resources, env_resources_labels)
        current_terminal.BHS_line_full = False
    yield env.timeout(current_terminal.BHS_time)

    if terminal != DepartureTerminal:
        if terminal in terminals_in_south:
            current_station = stations['south']
            st_label = 'south' 
        else:
            current_station = stations['north']
            st_label = 'north'
        #enter station line
        station_line_request = current_station.station_line.request()
        if current_station.station_line.count > current_station.QueryCapacity and not current_station.station_line_full:
            add_to_log("M001", TagID, current_station.label, "Station", "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_station.station_line_full = True
        yield station_line_request
        current_terminal.BHS.release(BHS_storage_request)
        if len(current_terminal.BHS.queue) == 0 and current_terminal.BHS_storage_full:
            add_to_log("M004", TagID, terminal, "BHS", "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.BHS_storage_full = False
        add_to_log("B002", TagID, current_station.label, "Station", "NA")
        add_to_system_log(env_resources, env_resources_labels)
        
        #enter station
        station_resource_request = current_station.station_resource.request()
        if len(current_station.station_resource.queue) > 0 and not current_station.station_resource_full:
            add_to_log("M003", TagID, current_station.label, 'Station', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_station.station_resource_full = True
        yield station_resource_request
        station_transfer_request = current_station.station_transfer.request()
        yield station_transfer_request
        current_station.station_line.release(station_line_request)
        yield env.timeout(10)
        current_station.station_transfer.release(station_transfer_request)
        add_to_log("B001", TagID, current_station.label, "Station", "NA")
        add_to_system_log(env_resources, env_resources_labels)
        
        if current_station.station_line.count <= current_station.QueryCapacity and current_station.station_line_full:
            add_to_log("M002", TagID, current_station.label, "Station", "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_station.station_line_full = False
        yield env.timeout(current_station.ProcessingTime)

        if check_stations(terminals_in_south, terminals_in_north, DepartureTerminal) == st_label:
            #enter line for the next BHS for another terminal in this station
            next_terminal = terminals[DepartureTerminal]
            yield env.timeout(current_station.ProcessingTime)
            BHS_line_request = next_terminal.BHS_line.request()
            if next_terminal.BHS_line.count > 40 and not next_terminal.BHS_line_full:
                add_to_log("M001", TagID, next_terminal.TerminalID, 'BHS', "NA")
                add_to_system_log(env_resources, env_resources_labels)
                next_terminal.BHS_line_full = True
            yield BHS_line_request
            current_station.station_resource.release(station_resource_request)
            if len(current_station.station_resource.queue) == 0 and current_station.station_resource_full:
                add_to_log("M004", TagID, current_station.label, "Station", "NA")
                add_to_system_log(env_resources, env_resources_labels)
                current_station.station_resource_full = False
            add_to_log("B002", TagID, next_terminal.TerminalID, 'BHS', "NA")
            add_to_system_log(env_resources, env_resources_labels)

            #try enter BHS
            BHS_storage_request = next_terminal.BHS.request()
            if len(next_terminal.BHS.queue) > 0 and not next_terminal.BHS_storage_full:
                add_to_log("M003", TagID, next_terminal.TerminalID, "BHS", "NA")
                add_to_system_log(env_resources, env_resources_labels)
                next_terminal.BHS_storage_full = True
            yield BHS_storage_request

            BHS_transfer_request = next_terminal.BHS_transfer.request()
            yield BHS_transfer_request
            next_terminal.BHS_line.release(BHS_line_request)
            yield env.timeout(time_to_restart)
            next_terminal.BHS_transfer.release(BHS_transfer_request)

            add_to_log('B001', TagID, next_terminal.TerminalID, 'BHS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            if next_terminal.BHS_line.count <= 40 and next_terminal.BHS_line_full:
                add_to_log("M002", TagID, next_terminal.TerminalID, 'BHS', "NA")
                add_to_system_log(env_resources, env_resources_labels)
                next_terminal.BHS_line_full = False
            yield env.timeout(next_terminal.BHS_time)
            # current_terminal = next_terminal

        else:
            if st_label == 'south':
                ctt = ctt_2
            else:
                ctt = ctt_1
            CTT_line_request = ctt.CTT_line.request()
            if ctt.CTT_line.count > ctt.QueryCapacity and not ctt.CTT_line_full:
                add_to_log("M001", TagID, ctt.label, "CTT", "NA")
                add_to_system_log(env_resources, env_resources_labels)
                ctt.CTT_line_full = True
            yield CTT_line_request
            current_station.station_resource.release(station_resource_request)
            if len(current_station.station_resource.queue) == 0 and current_station.station_resource_full:
                add_to_log("M004", TagID, current_station.label, "Station", "NA")
                add_to_system_log(env_resources, env_resources_labels)
                current_station.station_resource_full = False
            add_to_log("B002", TagID, ctt.label, "CTT", "NA")
            add_to_system_log(env_resources, env_resources_labels)

            #enter CTT
            CTT_resource_request = ctt.CTT_resource.request()
            if len(ctt.CTT_resource.queue) > 0 and not ctt.CTT_resource_full:
                add_to_log("M003", TagID, ctt.label, 'CTT', "NA")
                add_to_system_log(env_resources, env_resources_labels)
                ctt.CTT_resource_full = True
            yield CTT_resource_request
            CTT_transfer_request = ctt.CTT_transfer.request()
            yield CTT_transfer_request
            ctt.CTT_line.release(CTT_line_request)
            yield env.timeout(10)
            ctt.CTT_transfer.release(CTT_transfer_request)
            add_to_log("B001", TagID, ctt.label, "CTT", "NA")
            add_to_system_log(env_resources, env_resources_labels)

            if ctt.CTT_line.count <= ctt.QueryCapacity and ctt.CTT_line_full:
                add_to_log("M002", TagID, ctt.label, "CTT", "NA")
                add_to_system_log(env_resources, env_resources_labels)
                ctt.CTT_line_full = False
            yield env.timeout(ctt.ProcessingTime)

            if st_label == 'south':
                current_station = stations['north']
            else:
                current_station = stations['south']

            station_line_request = current_station.station_line.request()
            if current_station.station_line.count > current_station.QueryCapacity and not current_station.station_line_full:
                add_to_log("M001", TagID, current_station.label, "Station", "NA")
                add_to_system_log(env_resources, env_resources_labels)
                current_station.station_line_full = True
            yield station_line_request
            ctt.CTT_resource.release(CTT_resource_request)
            if len(ctt.CTT_resource.queue) == 0 and ctt.CTT_resource_full:
                add_to_log("M004", TagID, ctt.label, "CTT", "NA")
                add_to_system_log(env_resources, env_resources_labels)
                ctt.CTT_resource_full = False
            add_to_log("B002", TagID, current_station.label, "Station", "NA")
            add_to_system_log(env_resources, env_resources_labels)

            #enter station
            station_resource_request = current_station.station_resource.request()
            if len(current_station.station_resource.queue) > 0 and not current_station.station_resource_full:
                add_to_log("M003", TagID, current_station.label, 'Station', "NA")
                add_to_system_log(env_resources, env_resources_labels)
                current_station.station_resource_full = True
            yield station_resource_request
            station_transfer_request = current_station.station_transfer.request()
            yield station_transfer_request
            current_station.station_line.release(station_line_request)
            yield env.timeout(10)
            current_station.station_transfer.release(station_transfer_request)
            add_to_log("B001", TagID, current_station.label, "Station", "NA")
            add_to_system_log(env_resources, env_resources_labels)     

            if current_station.station_line.count <= current_station.QueryCapacity and current_station.station_line_full:
                add_to_log("M002", TagID, current_station.label, "Station", "NA")
                add_to_system_log(env_resources, env_resources_labels)
                current_station.station_line_full = False
            
            #enter another terminal
            next_terminal = terminals[DepartureTerminal]
            yield env.timeout(current_station.ProcessingTime)
            BHS_line_request = next_terminal.BHS_line.request()
            if next_terminal.BHS_line.count > 40 and not next_terminal.BHS_line_full:
                add_to_log("M001", TagID, next_terminal.TerminalID, 'BHS', "NA")
                add_to_system_log(env_resources, env_resources_labels)
                next_terminal.BHS_line_full = True
            yield BHS_line_request
            current_station.station_resource.release(station_resource_request)
            if len(current_station.station_resource.queue) == 0 and current_station.station_resource_full:
                add_to_log("M004", TagID, current_station.label, "Station", "NA")
                add_to_system_log(env_resources, env_resources_labels)
                current_station.station_resource_full = False
            add_to_log("B002", TagID, next_terminal.TerminalID, 'BHS', "NA")
            add_to_system_log(env_resources, env_resources_labels)

            #try enter BHS
            BHS_storage_request = next_terminal.BHS.request()
            if len(next_terminal.BHS.queue) > 0 and not next_terminal.BHS_storage_full:
                add_to_log("M003", TagID, next_terminal.TerminalID, "BHS", "NA")
                add_to_system_log(env_resources, env_resources_labels)
                next_terminal.BHS_storage_full = True
            yield BHS_storage_request

            BHS_transfer_request = next_terminal.BHS_transfer.request()
            yield BHS_transfer_request
            next_terminal.BHS_line.release(BHS_line_request)
            yield env.timeout(time_to_restart)
            next_terminal.BHS_transfer.release(BHS_transfer_request)

            add_to_log('B001', TagID, next_terminal.TerminalID, 'BHS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            if next_terminal.BHS_line.count <= 40 and next_terminal.BHS_line_full:
                add_to_log("M002", TagID, next_terminal.TerminalID, 'BHS', "NA")
                add_to_system_log(env_resources, env_resources_labels)
                next_terminal.BHS_line_full = False
            yield env.timeout(next_terminal.BHS_time)
            # current_terminal = next_terminal

    current_terminal = terminals[DepartureTerminal]
    terminal = current_terminal.TerminalID 

    # LTTBS (long term storage)
    if (arrow.get(DepDateTime, 'DD.MM.YY HH:mm:ss').timestamp() -  arrow.get(env.now).timestamp()) > A_lot_of_time:
        
        LTTBS_line_request = current_terminal.LTTBS_line.request()
        if current_terminal.LTTBS_line.count > 40 and not current_terminal.LTTBS_line_full:
            add_to_log('M001', TagID, current_terminal.TerminalID, 'LTTBS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.LTTBS_line_full = True
        yield LTTBS_line_request
            
            
        current_terminal.BHS.release(BHS_storage_request)
        if len(current_terminal.BHS.queue) == 0 and current_terminal.BHS_storage_full:
            add_to_log('M004', TagID, current_terminal.TerminalID, 'BHS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.BHS_storage_full = False
        add_to_log('B002', TagID, current_terminal.TerminalID, 'LTTBS', "NA")
        add_to_system_log(env_resources, env_resources_labels)
                
    #try enter LTTBS
        LTTBS_storage_request = current_terminal.LTTBS.request()
        if len(current_terminal.LTTBS.queue) > 0 and not current_terminal.LTTBS_storage_full:
            add_to_log('M003', TagID, current_terminal.TerminalID, 'LTTBS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.LTTBS_storage_full = True
        yield LTTBS_storage_request
        
        LTTBS_transfer_request = current_terminal.LTTBS_transfer.request()
        yield LTTBS_transfer_request
        current_terminal.LTTBS_line.release(LTTBS_line_request)
        yield env.timeout(time_to_restart)
        current_terminal.LTTBS_transfer.release(LTTBS_transfer_request)
                
    ## вышел из очереди на LTTBS встал в LTTBS
        add_to_log('B001', TagID, terminal, 'LTTBS', "NA")
        add_to_system_log(env_resources, env_resources_labels)
        add_to_system_log(env_resources, env_resources_labels)
        if current_terminal.LTTBS_line.count <= 40 and current_terminal.LTTBS_line_full:
            add_to_log('M002', TagID, current_terminal.TerminalID, 'LTTBS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.LTTBS_line_full = False
        yield env.timeout(current_terminal.LTTBS_time)
        if (arrow.get(DepDateTime, 'DD.MM.YY HH:mm:ss').timestamp() - Time_to_move - arrow.get(env.now).timestamp()) > 0:
            yield env.timeout(arrow.get(DepDateTime, 'DD.MM.YY HH:mm:ss').timestamp() - Time_to_move - arrow.get(env.now).timestamp())

        current_terminal.LTTBS.release(LTTBS_storage_request)
        if len(current_terminal.LTTBS.queue) == 0 and current_terminal.LTTBS_storage_full:
            add_to_log('M004', TagID, current_terminal.TerminalID, 'LTTBS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.LTTBS_storage_full = False

                        
                        
    #try enter BHS line
        BHS_line_request = current_terminal.BHS_line.request()
        if current_terminal.BHS_line.count > 40 and not current_terminal.BHS_line_full:
            add_to_log("M001", TagID, current_terminal.TerminalID, 'BHS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.BHS_line_full = True
        yield BHS_line_request
        current_terminal.LTTBS.release(LTTBS_storage_request)
        if len(current_terminal.LTTBS.queue) == 0 and current_terminal.LTTBS_storage_full:
            add_to_log("M004", TagID, current_terminal.TerminalID, "LTTBS", "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.LTTBS_storage_full = False
        add_to_log("B002", TagID, current_terminal.TerminalID, 'BHS', "NA")
        add_to_system_log(env_resources, env_resources_labels)

    #try enter BHS
        BHS_storage_request = current_terminal.BHS.request()
        if len(current_terminal.BHS.queue) > 0 and not current_terminal.BHS_storage_full:
            add_to_log("M003", TagID, current_terminal.TerminalID, "BHS", "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.BHS_storage_full = True
        yield BHS_storage_request
        
        BHS_transfer_request = current_terminal.BHS_transfer.request()
        yield BHS_transfer_request
        current_terminal.BHS_line.release(BHS_line_request)
        yield env.timeout(time_to_restart)
        current_terminal.BHS_transfer.release(BHS_transfer_request)
        
        add_to_log('B001', TagID, current_terminal.TerminalID, 'BHS', "NA")
        add_to_system_log(env_resources, env_resources_labels)
        if current_terminal.BHS_line.count <= 40 and current_terminal.BHS_line_full:
            add_to_log("M002", TagID, current_terminal.TerminalID, 'BHS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.BHS_line_full = False
        yield env.timeout(current_terminal.BHS_time)
        ## УРА МЫ опять В BHS

    ## TBS (short term storage)
    elif (arrow.get(DepDateTime, 'DD.MM.YY HH:mm:ss').timestamp() -  arrow.get(env.now).timestamp()) > Time_to_move:
        TBS_line_request = current_terminal.TBS_line.request()
        if current_terminal.TBS_line.count > 40 and not current_terminal.TBS_line_full:
            add_to_log('M001', TagID, current_terminal.TerminalID, 'TBS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.TBS_line_full = True
        yield TBS_line_request
            
            
        current_terminal.BHS.release(BHS_storage_request)
        if len(current_terminal.BHS.queue) == 0 and current_terminal.BHS_storage_full:
            add_to_log('M004', TagID, current_terminal.TerminalID, 'BHS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.BHS_storage_full = False
        add_to_log('B002', TagID, current_terminal.TerminalID, 'TBS', "NA")
        add_to_system_log(env_resources, env_resources_labels)

        #try enter TBS
        TBS_storage_request = current_terminal.TBS.request()
        if len(current_terminal.TBS.queue) > 0 and not current_terminal.TBS_storage_full:
            add_to_log('M003', TagID, current_terminal.TerminalID, 'TBS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.TBS_storage_full = True
        yield TBS_storage_request

        TBS_transfer_request = current_terminal.TBS_transfer.request()
        yield TBS_transfer_request

        current_terminal.TBS_line.release(TBS_line_request)
        yield env.timeout(time_to_restart)
        current_terminal.TBS_transfer.release(TBS_transfer_request)

    ## вышел из очереди на TBS встал в TBS
        add_to_log('B001', TagID, current_terminal.TerminalID, 'TBS', "NA")
        add_to_system_log(env_resources, env_resources_labels)

        if current_terminal.TBS_line.count <= 40 and current_terminal.TBS_line_full:
            add_to_log('M002', TagID, current_terminal.TerminalID, 'TBS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.TBS_line_full = False
    ## отстояли свое в TBS
        yield env.timeout(current_terminal.TBS_time)
        if (arrow.get(DepDateTime, 'DD.MM.YY HH:mm:ss').timestamp() - Time_to_move - arrow.get(env.now).timestamp()) > 0:
            yield env.timeout(arrow.get(DepDateTime, 'DD.MM.YY HH:mm:ss').timestamp() - Time_to_move - arrow.get(env.now).timestamp())
        current_terminal.TBS.release(TBS_storage_request)

        if len(current_terminal.TBS.queue) == 0 and current_terminal.TBS_storage_full:
            add_to_log('M004', TagID, terminal, 'TBS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.TBS_storage_full = False
                    
    #try enter BHS line
        BHS_line_request = current_terminal.BHS_line.request()
        if current_terminal.BHS_line.count > 40 and not current_terminal.BHS_line_full:
            add_to_log("M001", TagID, current_terminal.TerminalID, 'BHS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.BHS_line_full = True
        yield BHS_line_request
        current_terminal.TBS.release(TBS_storage_request)
        if len(current_terminal.TBS.queue) == 0 and current_terminal.TBS_storage_full:
            add_to_log("M004", TagID, current_terminal.TerminalID, "TBS", "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.TBS_storage_full = False
        add_to_log("B002", TagID, current_terminal.TerminalID, 'BHS', "NA")
        add_to_system_log(env_resources, env_resources_labels)

        #try enter BHS
        BHS_storage_request = current_terminal.BHS.request()
        if len(current_terminal.BHS.queue) > 0 and not current_terminal.BHS_storage_full:
            add_to_log("M003", TagID, current_terminal.TerminalID, "BHS", "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.BHS_storage_full = True
        yield BHS_storage_request
        
        BHS_transfer_request = current_terminal.BHS_transfer.request()
        yield BHS_transfer_request
        current_terminal.BHS_line.release(BHS_line_request)
        yield env.timeout(time_to_restart)
        current_terminal.BHS_transfer.release(BHS_transfer_request)
        
        add_to_log('B001', TagID, current_terminal.TerminalID, 'BHS', "NA")
        add_to_system_log(env_resources, env_resources_labels)
        if current_terminal.BHS_line.count <= 40 and current_terminal.BHS_line_full:
            add_to_log("M002", TagID, current_terminal.TerminalID, 'BHS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.BHS_line_full = False
        yield env.timeout(current_terminal.BHS_time)
        ## УРА МЫ опять В BHS

    #ЕДЕМ В САМОЛЕТ!
    if (arrow.get(DepDateTime, 'DD.MM.YY HH:mm:ss').timestamp() -  arrow.get(env.now).timestamp()) <= Time_to_move:

        #try enter OUTBP line
        OUTBP_line_request = current_terminal.OUTBP_line.request()
        if current_terminal.OUTBP_line.count > 40 and not current_terminal.OUTBP_line_full:
            add_to_log('M001', TagID, current_terminal.TerminalID, 'OUTBP', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.OUTBP_line_full = True
        yield OUTBP_line_request


        current_terminal.BHS.release(BHS_storage_request)
        if len(current_terminal.BHS.queue) == 0 and current_terminal.BHS_storage_full:
            add_to_log('M004', TagID, current_terminal.TerminalID, 'BHS', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.BHS_storage_full = False
        add_to_log('B002', TagID, current_terminal.TerminalID, 'OUTBP', "NA")
        add_to_system_log(env_resources, env_resources_labels)

        #try enter OUTBP
        OUTBP_storage_request = current_terminal.OUTBP.request()
        if len(current_terminal.OUTBP.queue) > 0 and not current_terminal.OUTBP_storage_full:
            add_to_log('M003', TagID, current_terminal.TerminalID, 'OUTBP', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.OUTBP_storage_full = True
        yield OUTBP_storage_request
        OUTBP_transfer_request = current_terminal.OUTBP_transfer.request()
        yield OUTBP_transfer_request
        current_terminal.OUTBP_line.release(OUTBP_line_request)
        yield env.timeout(time_to_restart)
        current_terminal.OUTBP_transfer.release(OUTBP_transfer_request)
    ## вышел из очереди на бхс встал в бхс
        add_to_log('B001', TagID, current_terminal.TerminalID, 'OUTBP', "NA")
        add_to_system_log(env_resources, env_resources_labels)

        if current_terminal.OUTBP_line.count <= 40 and current_terminal.OUTBP_line_full:
            add_to_log('M002', TagID, current_terminal.TerminalID, 'OUTBP', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current.OUTBP_line_full = False
        yield env.timeout(current_terminal.OUTBP_time)
        current_terminal.OUTBP.release(OUTBP_storage_request)
        if len(current_terminal.OUTBP.queue) == 0 and current_terminal.OUTBP_storage_full:
            add_to_log('M004', TagID, current_terminal.TerminalID, 'OUTBP', "NA")
            add_to_system_log(env_resources, env_resources_labels)
            current_terminal.OUTBP_storage_full = False
    ##проверяем опаздали или нет
        if arrow.get(env.now).timestamp() > arrow.get(DepDateTime, 'DD.MM.YY HH:mm:ss').timestamp():
            add_to_log("M005", TagID, current_terminal.TerminalID, 'NA', DepFlNum)
            add_to_system_log(env_resources, env_resources_labels)
        else:
            add_to_log("B003", TagID, current_terminal.TerminalID, 'NA', DepFlNum)
            add_to_log("B004", TagID, current_terminal.TerminalID, "NA", DepFlNum)
            add_to_system_log(env_resources, env_resources_labels)

def sim(df):
    for index, row in df.iterrows():
        time = arrow.get(row['ArrDateTime'], 'DD.MM.YY HH:mm:ss').timestamp()
        delay = time - start.timestamp()
        # print(delay)
        if delay == 0:
            env.process(plane_land(env, row['ArrDateTime'], row['TagID'], row['ArrTerminal'], row['ArrFlNum'],
                                   row['DepartureTerminal'], row['DepFlNum'], row['DepDateTime']))
        else:
            start_delayed(env, plane_land(env, row['ArrDateTime'], row['TagID'], row['ArrTerminal'], row['ArrFlNum'],
                                   row['DepartureTerminal'], row['DepFlNum'], row['DepDateTime']), delay)
        yield env.timeout(0)

env.process(sim(transit))
env.run()

log_df = pd.DataFrame.from_dict(log)

log_df["eventTypeID"].value_counts()

system_log_df = pd.DataFrame.from_dict(system_log)

log_df.to_csv("log.csv")

system_log_df.to_csv("system_log.csv")


NaN = np.nan
transit['first'] = NaN
transit['second'] = NaN
transit['third'] = NaN
transit['fourth'] = NaN
first = []
second = []
third = []
fourth = []
for index, row in transit.iterrows():
    first.append(row['ArrTerminal'])
    stat_label = check_stations(terminals_in_south, terminals_in_north, row['ArrTerminal'])
    if row['ArrTerminal'] == row['DepartureTerminal']:
        second.append(NaN)
        third.append(NaN)
        fourth.append(NaN)
    else:
        second.append(stat_label)
        if stat_label == check_stations(terminals_in_south, terminals_in_north, row['DepartureTerminal']):
            third.append(row['DepartureTerminal'])
            fourth.append(NaN)
        else:
            if stat_label == 'south':
                third.append('north')
            else:
                third.append('south')
            fourth.append(row['DepartureTerminal'])

transit['first'] = first
transit['second'] = second
transit['third'] = third
transit['fourth'] = fourth

transit_cols = ['TagID','first', 'second', 'third', 'fourth']
new_log = pd.merge(log_df, transit[transit_cols], on=['TagID'], how='left')
new_log.head()

new_log.to_csv("path_log.csv")