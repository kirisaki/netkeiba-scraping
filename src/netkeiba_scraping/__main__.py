import argparse

from .scraper import Scraper


def main():
    parser = argparse.ArgumentParser(description='netkeiba スクレイパー')
    parser.add_argument(
        '--from-year',
        type=int,
        default=2013,
        help='取得開始年 (デフォルト: 2013)',
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./output',
        help='出力ディレクトリ (デフォルト: ./output)',
    )
    args = parser.parse_args()

    scraper = Scraper(output_dir=args.output_dir, from_year=args.from_year)
    scraper.update()


if __name__ == '__main__':
    main()
