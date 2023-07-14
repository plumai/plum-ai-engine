import io

from PIL import Image, ImageOps


def resize_pic(input_data_bytes):

    b = io.BytesIO(input_data_bytes)
    img = Image.open(b)
    new_img = ImageOps.exif_transpose(image=img)
    new_img = new_img.resize((512, 512))
    new_b = io.BytesIO()
    new_img.save(new_b, format="PNG")
    new_data = new_b.getvalue()
    return new_data

