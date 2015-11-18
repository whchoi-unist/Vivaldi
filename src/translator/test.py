from vi2cu_translator.main import vi2cu_translator


code = """
def viv_add(data, x):
    b = 10                                                                                                                                                                                                                                   
    a = list(10)
    for elem in range(10):
        #a[elem] = elem
        b = 10

    cnt = x%10 
    return a[cnt] + data[x]
"""

_code = """
def mip(volume, x, y):                                                                                                
    step = 1.0                                                                                                        
    line_iter = orthogonal_iter(volume, x, y, step)                                                                   
                                                                                                                      
    max = 0                                                                                                           
    for elem in line_iter:                                                                                            
        val = linear_query_3d(volume, elem)                                                                           
        if max < val:max = val                                                                                        
    return max 
"""

print vi2cu_translator(code)[0]
