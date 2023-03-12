# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 15:22:30 2023

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




def producer(storage,i,empty,non_empty,limit): # Función de los procesos Producer
    for v in range(N):
        print (f"{current_process().name} produciendo paso {v}\n")
        empty.acquire()  # Esperamos a que el hueco del productor este vacío, i.e. tenga el valor -2
        storage[i] = random.randint(limit.value,limit.value+100) # Creamos e introducimos un nuevo valor en dicho hueco
        non_empty.release() # Enviamos una señal de que el hueco ya tiene un producto nuevo
    empty.acquire()
    storage[i] = -1 # Cuando finaliza el proceso, lo indicamos metiendo en el hueco un -1
    non_empty.release()
    print(f'{current_process().name} ha terminado \n')    
    
    
    
    
    
def proceed(buffer):        # Función auxiliar de Consumer : Nos indica cuando todos los procesos Producers
    a = False               # han terminado de producir
    for p in buffer:
        if p != -1:
            a = True
    return a


def find_min(storage):      # Función auxiliar de Consumer : Nos devuelve el mínimo valor del buffer
    try:
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


def consumer(storage, non_empty,empty,limit,result): # Función del proceso Consumer (o merge)
        for sem in non_empty: # Esperaremos a que todos los huecos estén llenos de productos para poder empezar a consumir
            sem.acquire()
        
        print(f'{current_process().name} listo para empezar a consumir')
        
        while proceed(storage):  # Mientras no hayan acabado de producir TODOS los productores
            minimum = find_min(storage)  # Buscamos el índice del mínimo de todos los productos en el buffer
            product = storage[minimum]   # Obtenemos el valor del mismo

            result.append(product)      # Añadimos el producto mínimo a la lista final
            print(f"{current_process().name} consumiendo {product}\n")
            storage[minimum] = -2       # Dejamos el hueco del producto obtenido vacío
            limit.value = product       # Actualizamos el valor límite para crear productos de forma creciente
            
            empty[minimum].release()    # Enviamos una señal de que el hueco ha quedado vacío
            non_empty[minimum].acquire() # Esperamos a que de nuevo se llene

def main():
    storage = Array('i', NPROD) # Buffer
    limit = Value('i',0)   # Valor que delimitará el rango de valores para crear el producto, así los obtenemos de forma creciente
    for i in range(NPROD):
        storage[i] = -2     # Iniciamos el buffer con todos los huecos vacíos
    print ("almacen inicial", storage[:])

    non_empty = [Semaphore(0) for i in range(NPROD)]  # Lista de semáfotos que nos indican huecos llenos de productos
    empty = [Lock() for i in range(NPROD)]            # Lista de semáforos que nos indican huecos vacíos
    manager = Manager()
    result =manager.list()      # lista final de consumiciones
    prodlst = [ Process(target=producer,              # Lista de procesos Producer, en total hay NPROD
                        name=f'Producer_{i}',
                        args=(storage,i,empty[i],non_empty[i],limit))
                for i in range(NPROD) ]

    merge = [Process(target=consumer,                 # Lista de un único proceso Consumer
                      name="Consumer", args = (storage, non_empty,empty,limit,result))]
    

    for p in prodlst + merge:
        p.start()
    

    for p in prodlst + merge:
        p.join()
        
        
    print(f"\n \nLa lista final de consumiciones es: {result}\n \n")

if __name__ == '__main__':
    main()