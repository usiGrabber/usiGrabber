from usigrabber.cli import app
from usigrabber.utils.setup import system_setup


def main():
    system_setup(is_main_process=True)

    app()


if __name__ == "__main__":
    main()
