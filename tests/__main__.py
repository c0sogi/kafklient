import unittest


def main():
    loader = unittest.defaultTestLoader
    tests = loader.discover(start_dir="tests", pattern="test_*.py", top_level_dir=None)
    runner = unittest.TextTestRunner()
    runner.run(tests)


if __name__ == "__main__":
    main()
