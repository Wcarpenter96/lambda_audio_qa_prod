import urllib
import numpy as np
import pandas as pd
import cv2 
from functools import partial
from multiprocessing import  Pool
import json
import sys
from shapely.geometry import Polygon
import os
import urllib.request
from pathlib import Path
import urllib
from urllib.request import Request, urlopen
import boto3
sys.path.append('../qa_images/')
from imgAnno import create_img

class create_img():
    ''' 
    INPUTS:
        - img_name = Image name or ID
        - elements = Elements from annotation that will annotate image
        - img_input = Path where image will be pulled from
        - img_output = Path of the image output
    OUTPUTS:
        - wordline() = Images of wordlines only BOXED and SHADED
        - wordbox() = Images of wordbox SHADED and wordline BOXED
        - wordline_wordbox() = Images of both wordbox and wordline are SHADED
        - def annotated_images() = Images of both wordbox and wordline BOXED
    EXAMPLE:
        create_img(img_name,elements,img_input,img_output).wordline()
        OR
        create_img_obj = create_img(img_name,elements,img_input,img_output)
        create_img_obj.wordline()
    '''

    def __init__(self, img_name, elements, img_input, img_output, jwt_token, link=False):
        self.img_name = img_name  # Image name or ID
        self.elements = elements  # Elements found in image
        self.img_input = img_input  # Image path or image used or url to image
        self.img_output = img_output  # Path to output image
        self.link = link  # Link defaults to False and searches for local files; If it's True it looks for image in url
        self.jwt_token = jwt_token

    def url_to_image(self, readFlag=cv2.IMREAD_COLOR):
        if 'http' not in self.img_input:
            img = cv2.imread(f'{self.img_input}/{self.img_name}.jpg', cv2.IMREAD_COLOR)
            return img

        elif 'http' in self.img_input:
            try:
                req = Request(self.img_input)
                resp = urlopen(req)
            except:
                req = Request(self.img_input)
                req.add_header('x-cf-jwt-token', self.jwt_token)
                resp = urlopen(req)
            image = np.asarray(bytearray(resp.read()), dtype="uint8")
            img = cv2.imdecode(image, readFlag)
            # return the image
            return img
            # resp = urllib.request.urlopen(self.img_input)
            # img = np.asarray(bytearray(resp.read()), dtype="uint8")
            # img = cv2.imdecode(img, readFlag)
            # #return the image
            # return img

    def draw_poly(self, element, img, color, thickness):
        new_coordinates = []
        for i in range(0, 4):
            new_coordinates.append(
                [element['polygon'][i]['x']*img.shape[1], element['polygon'][i]['y'] * img.shape[0]])
        cv2.polylines(
            img, [np.array(new_coordinates, np.int32)], True, color, thickness)

    def draw_fillpoly(self, element, img, overlay, color_line, color_shade, opacity, thickness):
        new_coordinates = []
        for i in range(0, 4):
            new_coordinates.append(
                [element['polygon'][i]['x']*img.shape[1], element['polygon'][i]['y'] * img.shape[0]])

        cv2.polylines(img, [np.array(new_coordinates, np.int32)],
                      True, color_line, thickness)
        cv2.fillPoly(
            overlay, [np.array(new_coordinates, np.int32)], (color_shade))
        cv2.addWeighted(overlay, opacity, img, 1 - opacity, 0, img)

    def draw_box(self, element, img, color, thickness):
        x = int(element['bbox']['x'] * img.shape[1])
        y = int(element['bbox']['y'] * img.shape[0])
        x2 = x + int(element['bbox']['width'] * img.shape[1])
        y2 = y + int(element['bbox']['height'] * img.shape[0])
        cv2.rectangle(img, (x, y), (x2, y2), color, thickness)

    def wordline(self):
        output_dir = '{}/Wordline'.format(self.img_output)
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        img = self.url_to_image()

        for element in self.elements:
            # color = (0,0,0)
            color_line = (0, 0, 0)
            color_shade = (0, 0, 0)
            overlay = img.copy()

            if element['class'] == 'WORD':
                continue

            elif element['class'] == "LINE":
                # color = (0, 128, 0)
                color_line = (0, 128, 0)
                color_shade = (0, 128, 0)
                opacity = 0.2
                thickness = 1
                self.draw_fillpoly(element, img, overlay,
                                   color_line, color_shade, opacity, thickness)

            elif element['class'] == "DOCUMENT_CONTENT_AREA":
                self.draw_box(element, img, (0, 255, 0), 2)

        cv2.imwrite(f'{output_dir}/{self.img_name}.jpg', img)
        print('Image {}.jpg of wordline shaded created\n'.format(self.img_name))

    def wordbox(self):
        output_dir = '{}/Wordbox'.format(self.img_output)
        # print(output_dir)
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        img = self.url_to_image()
        for element in self.elements:
            # color = (0,0,0)
            color_line = (0, 0, 0)
            color_shade = (0, 0, 0)
            overlay = img.copy()

            if element['class'] == 'WORD':
                color_line = (0, 0, 0)
                color_shade = (0, 0, 0)
                opacity = 0.5
                thickness = 2
                self.draw_fillpoly(element, img, overlay,
                                   color_line, color_shade, opacity, thickness)
                self.draw_poly(element, img, color_line, thickness)

            elif element['class'] == "LINE":
                color = (0, 128, 0)
                opacity = 0.0
                thickness = 2
                self.draw_poly(element, img, color, thickness)

            elif element['class'] == "DOCUMENT_CONTENT_AREA":
                self.draw_box(element, img, (0, 255, 0), 2)

        cv2.imwrite(f'{output_dir}/{self.img_name}.jpg', img)
        print('Image {}.jpg of wordbox shaded created\n'.format(self.img_name))

    def wordline_wordbox(self):
        output_dir = '{}/AllBoxShaded'.format(self.img_output)
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        img = self.url_to_image()
        for element in self.elements:
            color = (0, 0, 0)
            # overlay = self.url_to_image()
            overlay = img.copy()
            if element['class'] == 'WORD':
                color = (0, 0, 0)
                color_line = (0, 0, 0)
                color_shade = (0, 0, 0)
                opacity = 0.5
                thickness = 2
                self.draw_fillpoly(element, img, overlay,
                                   color_line, color_shade, opacity, thickness)

            elif element['class'] == "LINE":
                color = (0, 128, 0)
                color_line = (0, 128, 0)
                color_shade = (0, 128, 0)
                opacity = 0.0
                thickness = 2
                self.draw_fillpoly(element, img, overlay,
                                   color_line, color_shade, opacity, thickness)

            elif element['class'] == "DOCUMENT_CONTENT_AREA":
                self.draw_box(element, img, (0, 255, 0), 2)

        cv2.imwrite(f'{output_dir}/{self.img_name}.jpg', img)
        print(f'{output_dir}/{self.img_name}.jpg')

    def annotated_images(self):
        output_dir = f'{self.img_output}/AnnotatedImages'
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

        img = self.url_to_image()
        for element in self.elements:
            color = (0, 0, 0)

            if element['class'] == 'WORD':
                color = (0, 0, 255)
                self.draw_poly(element, img, color, 2)

            elif element['class'] == "LINE":
                color = (0, 128, 0)
                self.draw_poly(element, img, color, 2)

            elif element['class'] == "DOCUMENT_CONTENT_AREA":
                self.draw_box(element, img, (0, 255, 0), 2)

        cv2.imwrite(f'{output_dir}/{self.img_name}.jpg', img)
        print(f'Annotated image {self.img_name}.jpg created\n')


def url_to_image_2(url, readFlag=cv2.IMREAD_COLOR):\
        # download the image, convert it to a NumPy array, and then read
    # it into OpenCV format
    resp = urllib.request.urlopen(url)
    image = np.asarray(bytearray(resp.read()), dtype="uint8")
    image = cv2.imdecode(image, readFlag)
    # return the image
    return image

def read_img(path, filename):
    if 'http' not in 'test':
        try:
            img_path = f'{path}/ImageAssets/{filename}.jpg'
            img = cv2.imread(img_path, cv2.IMREAD_COLOR)
        except:
            print(f'-- Unable to read image {filename}')
            return False
    else:
        try:
            img = url_to_image_2(path)
        except:
            print(f'-- Unable to read image {filename}- url!')
    return img

def calculate_iou(poly_1, poly_2):
    try:
        union = poly_1.union(poly_2).area
    except:
        return 0.0
    try:
        intersection = poly_1.intersection(poly_2).area
    except:
        return 0.0
    if union == 0:
        return 0.0
    iou = intersection / union
    return iou

    
def calculate_overlap(inner_poly, outer_poly):
    try:
        area = inner_poly.area
    except:
        return 0.0
    try:
        intersection = inner_poly.intersection(outer_poly).area
    except:
        return 0.0
    if area == 0:
        return 0.0
    overlap = intersection / area
    return overlap


def write_json(anno, path, filename):
    json_path = f'{path}/Annotations/{filename}.json'
    with open(json_path, 'w') as f1:
        json.dump(anno, f1)


def parallelize(data, func, num_of_processes=16):
    data_split = np.array_split(data, num_of_processes)
    pool = Pool(num_of_processes)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data

def run_on_subset(func, data_subset):
    return data_subset.apply(func, axis=1)

def parallelize_on_rows(data, func, num_of_processes=16):
    return parallelize(data, partial(run_on_subset, func), num_of_processes)

df['image'] = parallelize_on_rows(df, main)

# /bin/bash
# Starting bash
# cd /tmp/pse
# Navigating to working folder
# tree
# List all directories and files in tree format
# python3 -m IPython
# Starting ipython
# aws s3 cp s3://bucketName/pathToObject pathToDirectoryInEC2
# Downloading objects from s3 buckets to EC2
# aws s3 cp s3://bucketName/pathToObject .
# Use a period at the end if downloading to current working directory

def init_session():
    session = boto3.Session()
    s3 = session.resource('s3')
    return(s3)


def listObjects(s3, bucket_name, filepath):
    """Get list of filepaths to objects in bucket_name, filepath

    Args:
        bucket_name (str): s3 bucket name
        filepath (str): Path to folder
    """
    try:
        list_object_key = [obj.key for obj in list(self.s3session.Bucket(self.bucket_name).objects.filter(Prefix=filepath)) if obj.key[-1] != '/']
        print(f'-- Found {len(list_object_key)} objects in {bucket_name}/{filepath}')
        return(list_object_key)
    except Exception as e:
        print(f'-- Encountered error when locating objects in {bucket_name}/{filepath} -- {e}')
        return([])


def downloadObject(s3, bucket_name, filepath, dst_dir):
    """Download from s3 Bucket

    Args:
        bucket_name (str): s3 bucket name
        filepath (str): Path to file in s3 Bucket
        dst_dir (str): Path to local folder
    """
    filename = os.path.basename(filepath)
    new_path = os.path.join(dst_dir, filename)
    try:
        s3.Bucket(bucket_name).download_file(filepath, new_path)
        return({'file': filename, 'key': filepath, 'downloaded_to': new_path, 'status': 'success'})
    except Exception as e:
        print(f'-- Encountered error when downloading {filename} -- {e}')
        return({'file': filename, 'key': filepath, 'status': e})


def uploadObject(s3, bucket_name, filepath, dst_dir):
    """Upload to s3 Bucket

    Args:
        bucket_name (str): s3 bucket name
        filepath (str): Path to local file that needs to be uploaded
        dst_dir (str): Path to s3 bucket
    """
    filename = os.path.basename(filepath)
    new_path = os.path.join(dst_dir, filename)
    try:
        s3.Bucket(bucket_name).upload_file(filepath, new_path)
        return({'filename': filename, 'uploaded_to': new_path, 'status': 'success', 'url': f'https://{bucket_name}.s3.amazonaws.com/{new_path}'})
    except Exception as e:
        print(f'-- ERROR: For {filename} -- {e}')
        return({'filename': filename, 'status': e})