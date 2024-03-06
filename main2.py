def max_l(a):
    max_values=[]
    for i in a:
        max_value=max(i)
        max_values.append(max_value)
    return sum(max_values)
a=[[23,4,5,6],[54,6,7],[9,6,3]]
print(max_l(a))