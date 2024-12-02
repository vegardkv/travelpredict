from entur_collector.dataanalysis.convertdata import save_to_csv, find_deviations


if __name__ == '__main__':
    dev = find_deviations("from-do")
    dev.to_csv('deviations.csv')

