import logging
import sys
from app.sdk.scraped_data_repository import ScrapedDataRepository
from app.setup import datetime_parser, setup, string_validator  # noqa: F401
from app.scraper import scrape
import pandas as pd  # noqa: F401

def main(
    case_study_name: str,
    job_id: int,
    tracer_id: str,
    latitude: str,
    longitude: str,
    start_date: str,
    end_date: str,
    resolution: int,
    data_type: str,
    file_dir: str,
    sentinel_client_id:str,
    sentinel_client_secret:str,
    kp_host: str,
    kp_port: str,
    kp_auth_token: str,
    kp_scheme: str,
    log_level: str = "WARNING"
) -> None:

    try:
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    
        if not all([case_study_name, job_id, tracer_id, latitude, longitude, sentinel_client_id, sentinel_client_secret]):
            raise ValueError(f"case_study_name, job_id, tracer_id, latitude, longitude and sentinel credentials must all be set.")

        string_variables = {
            "case_study_name": case_study_name,
            "job_id": job_id,
            "tracer_id": tracer_id,
            "latitude": latitude,
            "longitude": longitude
        }

        logger.info(f"Validating string variables:  {string_variables}")

        for name, value in string_variables.items():
            string_validator(f"{value}", name)

        logger.info(f"String variables validated successfully!")

        logger.info(f"Converting start_date, end_date, and interval to datetime objects")
        start_date_dt = datetime_parser(start_date)
        end_date_dt = datetime_parser(end_date)
        if start_date_dt > end_date_dt:
            raise ValueError(f"Start date must be before end date. Found: {start_date_dt} > {end_date_dt}.")
        
        kernel_planckster, protocol, file_repository = setup(
            job_id=job_id,
            logger=logger,
            kp_auth_token=kp_auth_token,
            kp_host=kp_host,
            kp_port=kp_port,
            kp_scheme=kp_scheme,
        )

        scraped_data_repository = ScrapedDataRepository(
            protocol=protocol,
            kernel_planckster=kernel_planckster,
            file_repository=file_repository,
        )

        logger.info(f"Scraper setup successfully for case study: {case_study_name}")
        logger.debug(f"__main__ function invoked with arguments: {locals()}")
    except Exception as e:
        logger.error(f"Error setting up scraper: {e}")
        sys.exit(1)

    logger.info(f"Scraping data for case study: {case_study_name}")

    scrape(
    case_study_name=case_study_name,
    job_id=job_id,
    tracer_id=tracer_id,
    scraped_data_repository=scraped_data_repository,
    log_level=log_level,
    latitude=latitude,
    longitude=longitude,  
    start_date=start_date_dt,
    end_date=end_date_dt,
    resolution=resolution,
    data_type=data_type,
    file_dir=file_dir,
    sentinel_client_id = sentinel_client_id,
    sentinel_client_secret = sentinel_client_secret,
    )

if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Scrape data from Sentinel datacollection.")

    parser.add_argument(
        "--case-study-name",
        type=str,
        default="webcam",
        help="The name of the case study",
    )

    parser.add_argument(
        "--job-id",
        type=int,
        default="1",
        help="The job id",
    )

    parser.add_argument(
        "--tracer-id",
        type=str,
        default="1",
        help="The tracer id",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        help="The log level to use when running the scraper. Possible values are DEBUG, INFO, WARNING, ERROR, CRITICAL. Set to WARNING by default.",
    )

    parser.add_argument(
        "--latitude",
        type=str,
        required=True,
        help="latitude of the location",
    )

    parser.add_argument(
        "--longitude",
        type=str,
        required=True,
        help="longitude of the location",
    )

    parser.add_argument(
        "--start_date",
        type=str,
        required=True,
        help="Start datetime in the format 'YYYY-MM-DDTHH:MM",
    )

    parser.add_argument(
        "--end_date",
        type=str,
        required=True,
        help="End datetime in the format 'YYYY-MM-DDTHH:MM",
    )
    
    parser.add_argument(
        "--resolution",
        type=int,
        default=10,
        help="scraper resolution",
    )
    
    parser.add_argument(
        "--data_type",
        type=str,
        required=True,
        help="data type for scraping",
    )
    
    parser.add_argument(
        "--sentinel_client_id",
        type=str,
        required=True,
        help="sentinel hub client id",
    )
    
    parser.add_argument(
        "--sentinel_client_secret",
        type=str,
        required=True,
        help="sentinel hub client secret",
    )
    
    parser.add_argument(
        "--kp_host",
        type=str,
        default="60",
        help="kp host",
    )

    parser.add_argument(
        "--kp_port",
        type=int,
        default="60",
        help="kp port",
    )

    parser.add_argument(
        "--kp_auth_token",
        type=str,
        default="60",
        help="kp auth token",
        )

    parser.add_argument(
        "--kp_scheme",
        type=str,
        default="http",
        help="kp scheme",
        )

    parser.add_argument(
        "--file_dir",
        type=str,
        default="./.tmp",
        help="saved file directory",
    )


    args = parser.parse_args()

    main(
        case_study_name=args.case_study_name,
        job_id=args.job_id,
        tracer_id=args.tracer_id,
        log_level=args.log_level,
        latitude=args.latitude,
        longitude=args.longitude,
        start_date=args.start_date,
        end_date=args.end_date,
        resolution=args.resolution,
        data_type=args.data_type,
        kp_host=args.kp_host,
        kp_port=args.kp_port,
        kp_auth_token=args.kp_auth_token,
        kp_scheme=args.kp_scheme,
        file_dir=args.file_dir,
        sentinel_client_id=args.sentinel_client_id,
        sentinel_client_secret=args.sentinel_client_secret,
    )