n=int(input("Enter a number:"))
for i in range(2,n+1):
    if n%i==0:
        pass
if i==n:
    print("is a prime")
else:
    print("not a prime")
# filter method
lst=[23,46,67,78]
def fun(x):
    if x%2==0:
        return True
    else:
        return False
evn_l =list(filter(fun,lst))
print(evn_l)
