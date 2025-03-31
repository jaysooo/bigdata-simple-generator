import yaml
from producer.pyarrow_producer import PyArrowProducer

from argparse import ArgumentParser

ProducerFactory = {
    'pyarrow': PyArrowProducer
}


class DataGenerator:
    def __init__(self, config_path: str,producer_type: str):
        document = open(config_path, 'r')
        self.config = yaml.load(document,Loader = yaml.SafeLoader)
        self.producer = ProducerFactory[producer_type](self.config)
    

    def run(self):
        data = self.producer.build_dataset()
        self.producer.produce(data)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--config", type=str, required=True , default='config.yaml')
    parser.add_argument("--producer", type=str, required=True , default='pyarrow')
    args = parser.parse_args()
    generator = DataGenerator(config_path=args.config, producer_type=args.producer)
    generator.run()


