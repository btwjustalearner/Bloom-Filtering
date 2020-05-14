# Bloom-Filtering

I implement the Bloom Filtering algorithm to estimate whether the city of a business in business_second.json has shown before in business_first.json. The details of the Bloom Filtering Algorithm can be found at the streaming lecture slide. You need to find proper bit array size, hash functions and the number of hash functions in the Bloom Filtering algorithm.

Some possible the hash functions are:

f(x)= (ax + b) % m or f(x) = ((ax + b) % p) % m

where p is any prime number and m is the length of the filter bit array. You can use any combination for the parameters (a, b, p). The hash functions should keep the same once you created them.

Since the city of a business is a string, you need to convert it into an integer and then apply hash functions to it., the following code shows one possible solution:

import binascii

int(binascii.hexlify(s.encode('utf8')),16)

(We only treat the exact the same strings as the same cities. You do not need to consider alias. If one record in the business_second.json file does not contain the city field, or the city field is empty, you should directly predict zero for that record.)
