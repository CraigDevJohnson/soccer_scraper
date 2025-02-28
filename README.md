# Soccer Scraper

A Python script for scraping soccer-related data, deployable as AWS Lambda function.

## Description

This project contains scripts to scrape and process soccer/football data from various sources. The data can be used for analysis, statistics, or other soccer-related applications.

## Local Installation

```bash
pip install -r requirements.txt
```

## Usage

### Local Usage

```python
python soccer_schedule_scraper.py
```

### AWS Lambda Usage

The script can be deployed as an AWS Lambda function. Call the function with:

```json
{
    "team_ids": ["123456", "654321"]
}
```

## Deployment

### AWS Lambda Deployment

1. Set up GitHub repository secrets:
   - AWS_ACCESS_KEY_ID
   - AWS_SECRET_ACCESS_KEY

2. Push to main branch to trigger automatic deployment, or manually trigger the workflow in GitHub Actions.

## Features

- Data scraping from soccer websites
- Data processing and cleaning
- Calendar export functionality
- AWS Lambda support with GitHub Actions deployment

## Dependencies

- Python 3.9+
- Required packages listed in requirements-lambda.txt

## License

MIT License

## Contributing

Pull requests are welcome. For major changes, please open an issue first.