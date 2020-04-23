from datetime import datetime
import main


def handler(event, context):
    try:
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Started at =", current_time)

        update()

        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Ended at =", current_time)
    except Exception as e:
        print(e)


def update():
    print('init...')
    main.main()


if __name__ == "__main__":

    try:
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Current Time =", current_time)

        update()

        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Current Time =", current_time)
    except Exception as e:
        print(e)
