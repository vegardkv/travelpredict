from entur_collector.dataanalysis.convertdata import save_to_csv, find_deviations
from datetime import datetime


if __name__ == '__main__':
    dev = find_deviations()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    dev.to_csv(f'deviations_{timestamp}.csv')

