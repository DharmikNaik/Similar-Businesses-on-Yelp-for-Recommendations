import os
from src.config_parser import ConfigParser
from src.constants import PROJECT_ROOT


def outputSimilarPairs(similarPairs, config: ConfigParser):
    outputFilepath = os.path.join(PROJECT_ROOT, config['file_paths']['output_path'])
    with open(outputFilepath, 'w') as file:
        file.write('business1, business2\n')
        for similarPair in similarPairs:
            business1 = similarPair[0]
            business2 = similarPair[1]
            file.write(f'{business1}, {business2}\n')