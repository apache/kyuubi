<div align=center>

![](../imgs/kyuubi_logo_simple.png)

</div>

# Building Kyuubi Documentation

Follow the steps below and learn how to build the Kyuubi documentation as the one you are watching now.

## Install & Activate `virtualenv`

Firstly, install `virtualenv`, this is optional but recommended as it is useful to create an independent environment to resolve dependency issues for building the documentation.

```bash
pip install virtualenv
```

Switch to the `docs` root directory.

```bash
cd $KTUUBI_HOME/docs
```

Create a virtual environment named 'kyuubi' or anything you like using `virtualenv` if it's not existing.

```bash
virtualenv kyuubi
```

Activate it,

```bash
 source ./kyuubi/bin/activate
```

## Install all dependencies

Install all dependencies enumerated in the `requirements.txt`

```bash
pip install -r requirements.txt
```

## Create Documentation

```bash
make html
```

If the build process succeed, the HTML pages are in `_build/html`.

## View Locally

Open the `_build/html/index.html` file in your favorite web browser.
