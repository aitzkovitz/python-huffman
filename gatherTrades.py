import filecmp
import sys
import requests
from collections import Counter
import json
from heapq import heapify, heappop, heappush
from bitarray import bitarray


TRADES_ENDPOINT = 'https://api.polygon.io/v2/ticks/stocks/trades'
TICKER = 'AAPL'
DATE = '2020-10-14' #ISO 8601
LIMIT = 50000

# Store our huffman code in a python dict.
CODE = {}

# Represents 1 node in our min heap which we use to generate a huffman code
class HuffCodeNode:
    def __init__(self, freq, symbol, left=None, right=None):
        self.left = left        # left child node
        self.right = right      # right child node
        self.freq = freq        # character frequency as float
        self.symbol = symbol    # character 
        self.dir = ''

    def __lt__(self, other):
        return self.freq < other.freq

    def __repr__(self):
        return f'({self.dir}, {self.freq})'

# NOT USED
# Eventually use to preprocess data before huffman
def format_trade(trade):
    return json.dumps(trade)


# Create a min heap from a frequency map of our long json string.
def generate_freq_map_heap(trades):

    freq_hash = Counter(trades)
    total = sum(freq_hash.values())

    heap = [HuffCodeNode(item[1]/total, item[0]) for item in freq_hash.most_common() ]
    heapify(heap)

    return heap
 
# Traverse the heap to get populate our code dictionary
def get_codes(node, val=''):
    
    new_val = val + str(node.dir)
 
    # recurse to left node
    if (node.left):
        get_codes(node.left, new_val)

    # recurse to left node
    if (node.right):
        get_codes(node.right, new_val)
 
    # base case: child node
    if(not node.right and not node.left):
        CODE[node.symbol] = new_val

# Encode by writing bit array to a file
def encode_using_huff(to_be_encoded, filename):

    # turn code strings into bitarrays for efficiency
    for key, val in CODE.items():
        CODE[key] = bitarray(val)

    # Encode using bitarray
    encoded_trades = bitarray()
    encoded_trades.encode(CODE, to_be_encoded)
    with open(filename, 'ab') as f:
        encoded_trades.tofile(f)

# Decode by reading bit array from a file
def decode_using_huff(file_to_be_decoded):

    # Decode using bitarray
    encoded_trades = bitarray()
    with open(file_to_be_decoded, 'rb') as fh:
        encoded_trades.fromfile(fh)
        # print(''.join(encoded_trades.decode(CODE)))
        return ''.join(encoded_trades.decode(CODE))


def main():

    if len(sys.argv) < 2:
        print('Please provide an API key as a command line argument.')
        sys.exit(-1)

    request_url = f"{TRADES_ENDPOINT}/{TICKER}/{DATE}"
    payload = {'reverse': 'false', 'limit': LIMIT, 'apiKey':sys.argv[1]}

    try:
        r = requests.get(request_url, params=payload)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        # Some sort of issue with the request
        raise SystemExit(e)
    
    
    total_results = []
    json_results = json.loads(r.text)
    num_results = json_results['results_count']

    if num_results < 1:
        print('No trades occured on the given day.')
        return


    last_ts = json_results['results'][-1]['t']
    total_results += json_results['results']

    while (1):

        # there isn't a next page
        if num_results < LIMIT:
            break

        # Add ts for pagination
        payload['timestamp'] = last_ts

        try:
            r = requests.get(request_url, params=payload)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            # Some sort of issue with the request
            raise SystemExit(e)

        json_results = json.loads(r.text)

        last_ts = json_results['results'][-1]['t']
        num_results = json_results['results_count']
        total_results += json_results['results']


    # with open('data.txt', 'a+') as f:
    #     f.write(json.dumps(total_results))

    # Get json list of results as a string and use to create a frequency map and min heap
    results_json_string = json.dumps(total_results)
    heap = generate_freq_map_heap(results_json_string)

    # Build codes by continuously merging 2 min values into new node and pushing that to heap
    i = 0
    while(len(heap) - 1):

        # Add codes, still need to deal with symbols with identical keys
        try:
            left_child = heappop(heap)
            left_child.dir = 0
            right_child = heappop(heap)
            right_child.dir = 1
            heappush(heap, HuffCodeNode(left_child.freq + right_child.freq, None, left_child, right_child))

            # There should be exactly n - 1 merges
            i -= 1
        except IndexError:
            break
    
    # Populate code dictionary
    get_codes(heap[0])

    print('Compressing API results into trades.bin... \n')
    encode_using_huff(results_json_string, 'trades.bin')
    print('Decompressing trades.bin... \n')
    decoded_string = decode_using_huff('trades.bin')

    with open('orig.txt', 'a+') as f:
        f.write(results_json_string)

    with open('decomp.txt', 'a+') as f:
        f.write(decoded_string)

    assert filecmp.cmp('orig.txt', 'decomp.txt')

    print('The files are identical.')


if __name__ == '__main__':
    main()
