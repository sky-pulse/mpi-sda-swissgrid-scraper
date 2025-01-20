from datetime import datetime, timedelta
from pprint import pprint
from app.sdk.models import KernelPlancksterSourceData, BaseJobState, JobOutput
from app.sdk.scraped_data_repository import KernelPlancksterSourceData, ScrapedDataRepository
import time
import numpy as np
from typing import List
import requests
from PIL import Image
from io import BytesIO
import logging
import time
import os
import shutil
from PIL import Image
import json
import pandas as pd
from app.utils import URL_TEMPLATE, generate_relative_path, get_webcam_info_from_name, get_webcam_name

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_image(): #TODO: Implement the logic based on usecase
    return None

def save_image(image, path, factor=1.0, clip_range=(0, 1)):
    """Save the image to the given path."""
    np_image = np.array(image) * factor
    np_image = np.clip(np_image, clip_range[0], clip_range[1])
    np_image = (np_image * 255).astype(np.uint8)
    Image.fromarray(np_image).save(path)

def scrape(case_study_name: str, job_id: int, tracer_id: str, scraped_data_repository: ScrapedDataRepository, log_level: str, file_dir: str) -> JobOutput:
    
    job_state = BaseJobState.CREATED

    start_time = time.time()
    image_path = None
    relative_path = None
    success = False
    
    try:
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=log_level)

        protocol = scraped_data_repository.protocol

        output_data_list: List[KernelPlancksterSourceData] = []

        logger.info(f"{job_id}: Starting Job")

        job_state = BaseJobState.RUNNING
        image_dir = os.path.join(file_dir, "images")
        os.makedirs(image_dir, exist_ok=True)
        image = fetch_image()
        current_date = datetime.now()
        unix_timestamp = int(current_date.timestamp())

        if image is None:
            relative_path = None
            raise Exception(f"Could not fetch image for {current_date}, with Unix timestamp {unix_timestamp}")
        
        file_extension = image.format.lower() if image.format else "png"
        image_filename = f"Swissgrid_test.{file_extension}"
        image_path = os.path.join(image_dir, "scraped", image_filename)
        os.makedirs(os.path.dirname(image_path), exist_ok=True)
        save_image(image, image_path, factor=1.5 / 255, clip_range=(0, 1))
        logger.info(f"Scraped Image at {time.time()} and saved to: {image_path}")
        dd_mm_yy = current_date.strftime("%d_%m_%y")
        
        relative_path = generate_relative_path(
            case_study_name=case_study_name,
            tracer_id=tracer_id,
            job_id=job_id,
            timestamp=unix_timestamp,
            dataset="swissgrid",      #dummy dataset name
            evalscript_name="swissgrid", #dummy evalscript name
            image_hash="nohash",
            file_extension=file_extension
        )

        media_data = KernelPlancksterSourceData(
            name="swissgrid", #dummy name
            protocol=protocol,
            relative_path=relative_path,
        )
        
        scraped_data_repository.register_scraped_photo(
            job_id=job_id,
            source_data=media_data,
            local_file_name=image_path,
        )

        output_data_list.append(media_data)
        success = True
    except Exception as error:
        logger.error(f"{job_id}: Unable to scrape data. Job with tracer_id {tracer_id} failed. Job state was '{job_state.value}' Error: {error}")
    
    finally:
        if image_path:
            if os.path.exists(image_path):
                try:
                    os.remove(image_path)
                    logger.info(f"Deleted scraped image at {image_path}")
                except Exception as e:
                    logger.warning(f"Could not delete scraped image: {e}")

    response_time = time.time() - start_time
    if success:
        logger.info(f"{job_id}: Job finished successfully. Response time: {response_time:.2f} seconds")
    return JobOutput(
        job_state=BaseJobState.FINISHED,
        tracer_id=tracer_id,
        source_data_list=output_data_list
    )

    
