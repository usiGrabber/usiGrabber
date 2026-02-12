from usigrabber.cli import app


# ALWAYS PROTECT APP BECAUSE OTHERWISE MULTIPROCESSING STARTS INFINITE INSTANCES!!!
def main():
    app()


if __name__ == "__main__":
    main()
