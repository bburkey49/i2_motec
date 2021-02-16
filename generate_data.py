import random
import itertools
import time


file_name = 'sample_data.txt'
data_points = 1000
random.seed(4)

measurements = {
    'car_data' : {
        'tags' : ['lap_number', 'driver'],
        'fields' : ['engine_temp','brake_temp', 'ground_speed', 'steering_angle', 'brake_pressure']
    },
}

def generate_tag_set(measurement):
    tags = measurement['tags']
    tag_set = ''
    for tag in tags:
        n = str(random.randint(1,20))
        tag_set += f'{tag}={n},'

    return tag_set[:-1]


def generate_field_set(measurement):
    fields = measurement['fields']
    field_set = ''
    for field in fields:
        n = str(random.random() * random.randint(1,100))
        field_set += f'{field}={n},'

    return field_set[:-1]

def generate_timestamp():
    current_ns = time.time_ns()
    one_day_ns = 86400000000000
    return random.randint(current_ns - one_day_ns, current_ns)
    1603044805017

def generate_data():
    data = []
    for measurement,meta_data in measurements.items():
        for _ in itertools.repeat(None, data_points):
            tag_set = generate_tag_set(meta_data)
            field_set = generate_field_set(meta_data)
            timestamp = generate_timestamp()
            data.append(f'{measurement},{tag_set} {field_set} {timestamp}\n')
    
    return data


with open(file_name, "w") as writer:

    writer.writelines(generate_data())



