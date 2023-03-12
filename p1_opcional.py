# -*- coding: utf-8 -*-
"""
Created on Sun Mar 12 11:01:17 2023

@author: marla
"""

from multiprocessing import Process, Manager
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from time import sleep
import random


N = 10
NPROD = 20
K=6




def add_data(storage,limit,i,mutex): #Función auxiliar de Producer: Cada vez que un productor cree un producto
                                     # lo mete a un hueco vacío (-2) en su correspondiente parte del buffer.
                                     
    mutex.acquire()    # Vamos a modificar estructuras compartidas entre los distintos procesos, luego usamos un Lock() para la exclusión mutua                     
    try:                                     
        value = random.randint(limit.value,limit.value+5)  # Creamos un valor 
        limit.value = value #para que el orden de creación de productos sea creciente
        j = i*K
        while storage[j] != -2: #Busca un hueco vacío en la parte del buffer correspondiente a ese proceso
            j += 1
        storage[j] = value # Introduce el valor en el hueco vacío
    finally:
        print(f'{current_process().name} ha producido el producto {value} \n')
        mutex.release()     # Ya hemos terminado de modificar las estructuras, dejamos posible paso a otros procesos para modificarlas

        
def producer(storage,limit,i,empty,non_empty,mutex):   #Función para los productores: en los argumentos, empty y non_empty son los semáforos
    for v in range(N):                                 # ya correspondientes al proceso en vivo
        empty.acquire()  # esperamos a que hay un hueco vacío en la parte del buffer del productor actual 
        print(f'{current_process().name} listo para producir paso {v}')
        
        add_data(storage,limit,i,mutex) # añadimos un elemento nuevo en el buffer
        non_empty.release() #mandamos una señal al consumidor de que el buffer contiene productos (no está vacío)
        
    empty.acquire()
    storage[(i+1)*K-1]=-1    # Cuando el proceso termina, lo indicamos metiendo un -1, y ya no va a crear más productos
    non_empty.release()                     
    print(f'{current_process().name} ha terminado\n')   
    


def proceed(storage,mutex): #Función auxiliar Consumer
    a = False               # Nos indica si han finalizado todos los procesos Producers
    i = 0                   # Esto ocurre cuando aparece un -1 al comienzo de cada parte
    while i < len(storage): # del buffer correspondiente a cada proceso Producer
        if storage[i] != -1:
            a = True
        i += K
    return a   
    
def find_min(storage,mutex):        #Función auxiliar de Consumer
    try:                            # Se encarga de buscar el mínimo de los elmentos en el buffer
        ind1 = 0
        mindex = 0
        melem = max(storage) + 1
        for elem in storage:
            if (elem < melem and elem > -1):
                melem = elem
                mindex = ind1
            ind1 += 1
    finally:   
        return mindex



def get_data(storage,result,minimum,mutex): #Función auxiliar de Consumer 
    mutex.acquire()                                   # Obtiene el mínimo y lo añade a la lista final que vamos a devolver
    try:                                              # Reorganiza el buffer de forma que sean los últimos huecos (de cada 
        result.append(storage[minimum])               # parte del buffer correspondiente a un productor) los que estén vacíos
        print(f'{current_process().name} ha consumido el producto {storage[minimum]}\n')

        for j in range(minimum,minimum+(K-1)):        

            storage[j] = storage[j+1]
        storage[minimum+K-1] = -2
         
    finally:
        mutex.release()
        

def consumer(storage,limit,non_empty,empty,result,mutex): #Función para el consumidor (o merge)
        for sem in non_empty: # debe esperar que todos los semáforos de todos los producers indiquen que cada parte 
            sem.acquire()     # que les corresponde del buffer no está vacía para iniciarse
            
        print(f'{current_process().name} listo para empezar a consumir \n')
            
        while proceed(storage,mutex): # Puede consumir si no han terminado de producir todos los producers (o sigue habiendo productos en el buffer)
            print(f'{current_process().name} listo para consumir')

            minimum = find_min(storage,mutex) # tomamos el índice del producto mínimo en el buffer
            get_data(storage,result,minimum, mutex) # añadimos el mínimo a la lista final y reorganizamos el buffer

            
            empty[minimum//K].release() # Mandamos una señal al producer del que hemos obtenido el elemento de que hay un hueco vacío
            non_empty[minimum//K].acquire() # Esperamos a que la parte del producer anterior no esté vacía entera




def main():
    storage = Array('i', NPROD*K) #buffer
    limit = Value('i',0)  # valor para crear productor de forma creciente
    for i in range(NPROD*K): # iniciamos el buffer con huecos vacíos
        storage[i] = -2
    print (f"Storage inicial {storage[:]} \n")
    


    non_empty = [Semaphore(0) for i in range(NPROD)] #Semáforos para indicar que hay productos en el buffer (nos sirve con que cada productor tenga al menos un hueco lleno)
    empty = [Semaphore(K) for i in range(NPROD)] #Semáfotos para indiciar que hay huecos vacíos en el buffer, para cada productor hay hasta K huecos
    mutex = Lock()  # Semáfoto para manipular la estructura compartida sin que se metan dos procesos a la vez 
    manager = Manager()
    result = manager.list() # Lista definitiva
    prodlst = [ Process(target=producer,    #Lista de procesos de productores, total de NPROD
                        name=f'Producer_{i}',
                        args=(storage,limit,i,empty[i],non_empty[i],mutex))
                for i in range(NPROD) ]

    merge = [Process(target=consumer,       # Lista de un único proceso consumidor (o merge)
                      name="Consumer", args = (storage,limit, non_empty,empty,result,mutex))]
    

    for p in prodlst + merge:
        p.start()
    

    for p in prodlst + merge:
        p.join()
        
        
    print(f"\n \nLa lista final de consumiciones es: {result}\n \n")

if __name__ == '__main__':
    main()