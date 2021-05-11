import multiprocessing
import multiprocessing as mp
import ctypes
import numpy as np
import pandas as pd
import time
import sys
import pandas as pd
import datetime
import random
from ast import literal_eval
from itertools import product
from functools import partial
from itertools import repeat
from collections import Counter



def getChangesByPlace(placeInfo):    
    def checkQueueChanges(tmpQueueChanges, tmpDelta, ind, state):
        if tmpDelta not in tmpQueueChanges:
            tmpQueueChanges[tmpDelta] = np.array([[ind, state]])
        else:
            tmpQueueChanges[tmpDelta] = np.vstack([tmpQueueChanges[tmpDelta], [ind, state]])
        return tmpQueueChanges

    def infect(individuals, place, currentTick):

        tmpQueueChanges = {}
        # print("Probable infected")
        for ind in individuals:
            if shared_array_People[ind, 2] == 0:
                params = loadParams()
                individualAge = int(shared_array_People[ind, 1])

                probabilities = np.random.uniform(size=6)
                if probabilities[0] < params['alpha'][individualAge]:
                    #print("infected!") 
                    shared_array_People[ind, 0] = 1
                    tmpDelta = currentTick + datetime.timedelta(days = params['T_E'][individualAge])         
                    if probabilities[1] < params['beta'][individualAge]:

                        tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 2.1)
                        tmpDelta = tmpDelta + datetime.timedelta(days = params['T_Ia'][individualAge]) 
                        
                        if probabilities[2] < params['gamma'][individualAge]:                    
                            tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 2.2)
                            tmpDelta = tmpDelta + datetime.timedelta(days = params['T_Is'][individualAge])  
                            
                            if probabilities[3] < params['theta'][individualAge]:
                                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 2.3)
                                tmpDelta = tmpDelta + datetime.timedelta(days = params['T_Ic'][individualAge]) 
                                
                                if probabilities[4] < params['phi'][individualAge]:
                                    tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 4)
                                else:
                                    tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 3)
                                    tmpDelta = tmpDelta + datetime.timedelta(days = params['T_R'][individualAge]) 
                                    
                                    if probabilities[5] < params['omega'][individualAge]: 
                                        tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 5)
                                    else:
                                        tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 0)
                                
                            else:
                                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 3)
                                tmpDelta = tmpDelta + datetime.timedelta(days = params['T_R'][individualAge]) 
                                
                                if probabilities[5] < params['omega'][individualAge]: 
                                    tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 5)
                                else:
                                    tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 0)
                        else:
                            tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 3)
                            tmpDelta = tmpDelta + datetime.timedelta(days = params['T_R'][individualAge]) 
                            
                            if probabilities[5] < params['omega'][individualAge]: 
                                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 5)
                            else:
                                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 0)
                    else:
                        tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 2.2)
                        tmpDelta = tmpDelta + datetime.timedelta(days = params['T_Is'][individualAge]) 

                        if probabilities[3] < params['theta'][individualAge]:
                            tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 2.3)
                            tmpDelta = tmpDelta + datetime.timedelta(days = params['T_Ic'][individualAge]) 
                            
                            if probabilities[4] < params['phi'][individualAge]:
                                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 4)
                            else:
                                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 3)
                                tmpDelta = tmpDelta + datetime.timedelta(days = params['T_R'][individualAge]) 
                                
                                if probabilities[5] < params['omega'][individualAge]: 
                                    tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 5)
                                else:
                                    tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 0)
                            
                        else:
                            tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 3)
                            tmpDelta = tmpDelta + datetime.timedelta(days = params['T_R'][individualAge]) 
                            
                            if probabilities[5] < params['omega'][individualAge]: 
                                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 5)
                            else:
                                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 0)


        return tmpQueueChanges

    place       = placeInfo[0]
    indINplace  = np.array(placeInfo[1])
    currentTick = placeInfo[2]
    #print(shared_array_People[[0, 2]])
    #print(indINplace)
    #individualsState = np.array([shared_array_People[i] for i in indINplace])
    individualsState = shared_array_People[indINplace, 0]
    # print(individualsState)
    if individualsState.shape[0] > 1 and 2.1 in individualsState and 0 in individualsState:
        #print(np.where(individualsState == 0))
        # print(indINplace[[0, 1]])
        individuals = indINplace[np.where(individualsState == 0)]
        tmpIndex = 14005 + int(place) # To find based in the Place ID in the shared memory
        meanContact = shared_array_Places[tmpIndex]
        #print(meanContact, indINplace, individualsState, individuals)
        if meanContact < len(individuals):
            individuals = np.random.choice(individuals, int(meanContact))
        # print(indINplace, individualsState, individuals)
        # print(individuals, place)
        tmpInfected = infect(individuals, place, currentTick)
        return tmpInfected
    else:
        return {}

def runOneDay(realQueueChanges):
    # print(len(queueChanges))
    
    def updateQueueChanges(realQueueChanges, tmpQueueChanges):
        for key in tmpQueueChanges:
            if key not in realQueueChanges:
                # print(key)
                realQueueChanges[key] = tmpQueueChanges[key]
            else:
                realQueueChanges[key] = np.vstack([realQueueChanges[key], tmpQueueChanges[key]])
        return realQueueChanges

    for tick in ticks:
        start = time.time()

        if tick in realQueueChanges:
            shared_array_People[realQueueChanges[tick][:, 0].astype(int), 0]  = realQueueChanges[tick][:, 1]
            #print(np.unique(shared_array_People))
            del realQueueChanges[tick]

        currentPlaces = newData[tick].dropna() #value_counts().keys()
        placesInfo = [[x, y, tick] for x, y in zip(currentPlaces.index, 
                                            currentPlaces.to_numpy())]

        '''
        pool = mp.Pool(20)
        results = pool.map(getChangesByPlace, placesInfo)
        pool.close()

        for tmpChangesByPlace in results:
            if len(tmpChangesByPlace) != 0:
                realQueueChanges  = updateQueueChanges(realQueueChanges, tmpChangesByPlace)
        '''
        
        for x in placesInfo:
            tmpChangesByPlace = getChangesByPlace(x)
            if len(tmpChangesByPlace) != 0:
                #print(tmpChangesByPlace.keys())
                realQueueChanges = updateQueueChanges(realQueueChanges, tmpChangesByPlace)
        
        # Statistics
        if tick.hour % 6 == 0 and tick.minute == 0:
            unique, counts = np.unique(shared_array_People[:, 0], return_counts=True)
            statistics[tick] = {state: count for state, count in zip(unique, counts)}        
            #print(dataPeople.head())
            statisticsUPZ[tick] = {}
            for upz in tmpUPZ.index:
                statisticsUPZ[tick][upz] = dict(Counter(shared_array_People[tmpUPZ.loc[upz], 0]))

            #sys.exit()
        end = time.time()
        #print("--- by tick", end - start)
    return realQueueChanges


def loadParams(): 
    params = {'alpha' : [0.000] + [0.37]*5, 
              'beta'  : [0.000] + [0.000, 0.800, 0.800, 0.200, 0.200], 
              'gamma' : [0.000] + [0.000, 0.008, 0.058, 0.195, 0.350],
              'theta' : [0.000] + [0.050, 0.050, 0.050, 0.198, 0.575], 
              'phi'   : [0.000] + [0.400, 0.400, 0.500, 0.500, 0.500], 
              'omega' : [0.000] + [0.900]*5, 
              #'omega' : [0.000] + [0.01]*5, 
              'T_E'   : np.round(np.random.gamma(5.1, 0.86, 6), 0), 
              'T_Ia'  : [0] + [3, 14, 14, 5, 5], 
              'T_Is'  : np.round(np.random.triangular(7, 8, 9, 6), 0), 
              'T_Ic'  : np.round(np.random.triangular(5, 7, 12, 6), 0),   
              'T_R'   : np.round(np.random.uniform(80, 100, 6), 0)}

    return params


def initialSchedule(individuals, indexIndividuals):

    def checkQueueChanges(tmpQueueChanges, tmpDelta, ind, state):
        if tmpDelta not in tmpQueueChanges:
            tmpQueueChanges[tmpDelta] = np.array([[ind, state]])
        else:
            tmpQueueChanges[tmpDelta] = np.vstack([tmpQueueChanges[tmpDelta], [ind, state]])
        return tmpQueueChanges


    tmpQueueChanges = {}
    for ind in indexIndividuals:
        params = loadParams()
        # print(params)
        individualAge = int(individuals[ind][1])
        individuals[ind] = [2.1, individualAge, individuals[ind][2]]
        tmpDelta = initialTime + datetime.timedelta(days = params['T_Ia'][individualAge])      
        probabilities = np.random.uniform(size=4)
        if probabilities[0] < params['gamma'][individualAge]:                    
            tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 2.2)
            tmpDelta = tmpDelta + datetime.timedelta(days = params['T_Is'][individualAge])  

            if probabilities[1] < params['theta'][individualAge]:
                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 2.3)
                tmpDelta = tmpDelta + datetime.timedelta(days = params['T_Ic'][individualAge]) 

                if probabilities[2] < params['phi'][individualAge]:
                    tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 4)
                else:
                    tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 3)
                    tmpDelta = tmpDelta + datetime.timedelta(days = params['T_R'][individualAge])   
                    
                    if probabilities[3]< params['omega'][individualAge]: 
                        tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 5)
                    else:
                        tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 0)

            else:
                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 3)
                tmpDelta = tmpDelta + datetime.timedelta(days = params['T_R'][individualAge]) 
                
                if probabilities[3] < params['omega'][individualAge]: 
                    tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 5)
                else:
                    tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 0)
        else:
            tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 3)
            tmpDelta = tmpDelta + datetime.timedelta(days = params['T_R'][individualAge]) 
            
            if probabilities[3] < params['omega'][individualAge]: 
                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 5)
            else:
                tmpQueueChanges = checkQueueChanges(tmpQueueChanges, tmpDelta, ind, 0)
    
    return individuals, tmpQueueChanges


def getDataPeople(filePeople, big): 
    if big:
        #dataPeople = pd.read_csv(filePeople, index_col=0).reset_index()
        dataPeople = pd.read_pickle(filePeople)
    else:
        dataPeople = pd.read_csv(filePeople, index_col=0, header=None).reset_index()
        dataPeople.columns = ['cod_upz', 'age', 'gender', 'id']
    dataPeople['state'] = 0
    dataPeople['individual'] = dataPeople.index
    print("Data people loaded!, dim ", dataPeople.shape)
    return dataPeople


def getDataPlaces(filePlaces, fileTrasmilenio):
    dataTrasmilenio = pd.read_excel(fileTrasmilenio)
    dataTrasmilenio = dataTrasmilenio[['numero_estacion', 'X', 'Y', 'nombre_estacion' ]]
    dataTrasmilenio['type'] = 'B'
    dataTrasmilenio.columns = ['id', 'x', 'y', 'name', 'type']
    dataTrasmilenio['id'] = -dataTrasmilenio['id']

    dataPlaces = pd.read_csv(filePlaces, index_col=0).reset_index()
    dataPlaces.columns = ['id', 'x', 'y', 'type']
    dataTrasmilenio = pd.read_excel(fileTrasmilenio)
    dataTrasmilenio = dataTrasmilenio[['numero_estacion', 'X', 'Y']]
    dataTrasmilenio['type'] = 'B'
    dataTrasmilenio.columns = ['id', 'x', 'y', 'type']
    dataTrasmilenio['id'] = -dataTrasmilenio['id']
    dataTrasmilenio = dataTrasmilenio.drop_duplicates(subset ="id") 
    dataPlacesComplete = pd.concat([dataTrasmilenio, dataPlaces])

    meanContacts  = {'H': 1 ,
                    'B': 2 , 
                    'W': 1 , 
                    'M': 1 , 
                    'S': 2 ,
                    'T': 2 ,}

    dataPlacesComplete['Mean contact'] = dataPlacesComplete['type'].apply(lambda x: meanContacts[x])
    dataPlacesComplete = dataPlacesComplete.set_index('id')
    dataPlacesComplete.index.name = None
    dataPlacesComplete = dataPlacesComplete.sort_index()
    
    print("Data places loaded!, dim", dataPlaces.shape)
    return dataPlacesComplete


def getIndividualMovements(big=False):
    if big:
        filePlacesChanging = "INFEKTA/Data/places_changing_upz_partial.pkl"
        newData = pd.read_pickle(filePlacesChanging)
    else:
        newData = pd.read_csv('INFEKTA/Data/places_changing.csv', header=None, index_col=0)
        newData = newData.applymap(lambda x: x if type(x) == float else literal_eval(x))
    
    print("Data per tick loaded!, dim ", newData.shape)
    return newData


def loadData(big, initialTime):
    if big:
        #dataPeople = getDataPeople("INFEKTA/Data/people_with_routes_UPZ_v2.csv", big)
        dataPeople = getDataPeople("./Data/people_with_routes_UPZ.pkl", big)
        dataPlaces = getDataPlaces('./Data/places_upz_v2.csv', 
                                    './Data/transmilenio.xlsx') 
    else:
        dataPeople = getDataPeople("./Data/people.csv", big)
        dataPlaces = getDataPlaces('./Data/places.csv',
                                    './Data/transmilenio.xlsx') 
    newData    = getIndividualMovements(big=big)
    #print(dataPeople['cod_upz'].value_counts())
    #sys.exit()

    #ticks  = []
    #currentTime = initialTime
    #for index in range(newData.shape[1]):
        #ticks.append(currentTime)
        #currentTime += datetime.timedelta(minutes=timeDelta)

    # Replace depending which time do you want to run
    #newData.columns = ticks
    # ticks   = ticks[48:120] + ticks[168:240]
    #print(newData.keys())
    #print(ticks)
    #newData = newData[ticks]

    return dataPeople, dataPlaces, newData, newData.keys()


if __name__ == '__main__':
    #######################################################################
    # Initial Parameters
    #######################################################################
    experimentID               = sys.argv[1]
    days                       = int(sys.argv[2])
    initialInfected            = int(sys.argv[3])
    timeDelta                  = 5 # 5 min
    initialTime                = datetime.datetime(2020, 1, 1, 0, 0)
    quarentinePercentagePeople = float(sys.argv[4])
    quarentineDayStart         = int(sys.argv[5])
    quarentineDayEnds          = int(sys.argv[6])

    dataPeople, dataPlaces, newData, ticks = loadData(True, initialTime)
    # print(dataPeople['age'].value_counts())

    #print(dataPeople.head(10))
    #print(newData.head(10))

    #sys.exit()
    indexInitialInfected = dataPeople.sample(initialInfected).index

    # dataPeople.loc[indexInitialInfected, 'state']  = [2.1 for _ in range(initialInfected)]
    # realQueueChanges = initialSchedule(indexInitialInfected)

    peopleFixed  = dataPeople[['state', 'age']].to_numpy(dtype = float) 
    inQuarentine = np.array([[0 for i in range(dataPeople.shape[0])]]).T
    peopleFixed  = np.concatenate((peopleFixed, inQuarentine), axis=1)

    #print(peopleFixed)
    peopleFixed, realQueueChanges  = initialSchedule(peopleFixed, indexInitialInfected)
    # peopleFixed  = np.append(peopleFixed, [[0, 1]]) 
    # print(peopleFixed.shape)
    #unique, counts = np.unique(peopleFixed[:, 2], return_counts=True)
    #print(unique, counts)
    

    placesFixed = np.zeros((abs(int(newData.index.min()) - int(newData.index.max()))+1, ))

    print(newData.index.min(), newData.index.max(), newData.shape, placesFixed.shape)
    dataPlacesContact = dataPlaces['Mean contact'].to_dict()

    tmpMin = min(dataPlacesContact)

    for index in newData.index:
        tmpIndex = int((-1)*tmpMin + index)
        placesFixed[tmpIndex] = dataPlacesContact[index]

    '''
    shared_array_People = multiprocessing.Array(ctypes.c_double, peopleFixed.shape[0]*3)
    shared_array_People = np.ctypeslib.as_array(shared_array_People.get_obj())
    shared_array_People = shared_array_People.reshape(peopleFixed.shape[0], 3)

    shared_array_Places = multiprocessing.Array(ctypes.c_double, placesFixed.shape[0])

    shared_array_People = peopleFixed
    shared_array_Places = placesFixed
    '''

    shared_array_People = peopleFixed.copy()
    shared_array_Places = placesFixed.copy()

    # shared_array_People[:, 2] = np.array([1 if random.random() < quarentinePercentagePeople else 0 for i in range(dataPeople.shape[0])])
    print(shared_array_People)
    #sys.exit()

    statistics = {}
    statistics[initialTime] = dataPeople['state'].value_counts().to_dict()
    
    statisticsUPZ = {}
    tmpUPZ = dataPeople.groupby('cod_upz')['individual'].apply(list)
    statisticsUPZ[initialTime] = {}
    for upz in tmpUPZ.index:
        statisticsUPZ[initialTime][upz] = {}

    for upz in tmpUPZ.index:
        statisticsUPZ[initialTime][upz] = dict(Counter(shared_array_People[tmpUPZ.loc[upz], 0]))

    #statisticsUPZ = pd.DataFrame.from_dict(statisticsUPZ)
    #print(statisticsUPZ)

    for _ in range(days):
        if _ == quarentineDayStart and quarentinePercentagePeople > 0:
            shared_array_People[:, 2] = np.array([1 if random.random() < quarentinePercentagePeople else 0 for i in range(dataPeople.shape[0])])
            print(shared_array_People)

        if _ == quarentineDayEnds and quarentinePercentagePeople > 0:
            shared_array_People[:, 2] = np.array([0 for i in range(dataPeople.shape[0])])
            print(shared_array_People)

        print(ticks[0])
        
        start_time = time.time()
        realQueueChanges = runOneDay(realQueueChanges)

        print(len(realQueueChanges))
        #print(realQueueChanges.keys())
        
        initialTime = initialTime + datetime.timedelta(days = 1)
        ticks = [tk + datetime.timedelta(days = 1) for tk in ticks]
        newData.columns = ticks

        print("--- %s seconds ---" % (time.time() - start_time))

    statistics = pd.DataFrame.from_dict(statistics)
    statisticsUPZ = pd.DataFrame.from_dict(statisticsUPZ)

    statistics.to_excel("INFEKTA/ResultsPLoS/statistics_e" + str(experimentID) + "_d" + str(days) +  "_i" + str(initialInfected) +  "_q" + str(quarentinePercentagePeople)  + "_qs" + str(quarentineDayStart) + "_qe" + str(quarentineDayEnds) +  ".xlsx")
    statisticsUPZ.to_pickle("INFEKTA/ResultsPLoS/statisticsUPZ_e" + str(experimentID) + "_d" + str(days) +  "_i" + str(initialInfected) +  "_q" + str(quarentinePercentagePeople)  + "_qs" + str(quarentineDayStart) + "_qe" + str(quarentineDayEnds) + ".pkl")



