from processing import load_latest_csv, remove_offsetting_adjustments, write_html_report


def main():
    df = load_latest_csv()

    print(f"\nTotal rows loaded: {len(df)}")

    flagged, removed = remove_offsetting_adjustments(df)

    print(f"Rows remaining after filter: {len(flagged)}")

    out_path = write_html_report(flagged, removed)
    print(f"\nReport written to: {out_path}")


if __name__ == "__main__":
    main()
