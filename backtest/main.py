from btManager import BackTestManager


if __name__ == '__main__':
    backTestManager = BackTestManager()
    backTestManager.excuteByLiveData('BBWCURVEStrategy')
