from multiprocessing.pool import ThreadPool
from threading import Thread

def thread_function(fun,arg_lst,nb_threads=1):
    threads=[]
    for args in arg_lst:
        t=Thread(target=fun,args=args)
        t.start()
        threads.append(t)
        if len(threads)>=nb_threads:
            for th in threads:
                th.join()
            threads=[]
    if len(threads)>0:
        for th in threads:
            th.join()

def thread_pool_function(fun,arg_lst,nb_threads=1):
    pool=ThreadPool(processes=nb_threads)
    threads=[]
    results=[]
    for args in arg_lst:
        t=pool.apply_async(fun,args)
        threads.append((args,t))
        if len(threads)>=nb_threads:
            for args,th in threads:
                results.append((args,th.get()))
            threads=[]
    if len(threads)>0:
        for args,th in threads:
            results.append((args,th.get()))
    return results