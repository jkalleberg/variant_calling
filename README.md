<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a id="readme-top"></a>
<!--
*** Thanks for checking out the Best-README-Template. See: https://github.com/othneildrew/Best-README-Template 
-->

<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <h3 align="center">Generic Variant Caller Pipeline</h3>

  <p align="center">
    An awesome README template to jumpstart your projects!
    <br />
    <a href="https://github.com/jkalleberg/variant_calling/tree/working-jk/docs"><strong>Explore the docs »</strong></a>
    <br />
  </p>
</div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>


<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple example steps.

### Prerequisites

This is an example of how to list things you need to use the software.
* npm
  ```sh
  npm install npm@latest -g
  ```

### Installation

_Below is an example of how you can install the app on the Hellbender HPC cluster._

1. Change directories to the working directory where the source code will be stored
    ```sh
    cd /mnt/pixstor/schnabelr-ccgi-drii/WORKING/jakth2
    ```

2. Clone the repo -- creating a local copy of the source code
    _Editing files within this cloned repository can create a tangled knot of Git commits if you don't know what you're doing! If you anticipate making edits to the code, you will need to **FORK** the repository (create your own version)._
    ```sh
    git clone git@github.com:jkalleberg/variant_calling.git
    ```

3. Enter the source code directory, and confirm the clone was successful.
    ```sh
    cd variant_calling
    git status
    ```

    **Expected Output:**
    ```sh
    On branch main
    Your branch is up to date with 'origin/main'.

    nothing to commit, working tree clean
    ```

4. Change a new git branch to avoid accidental pushes to base project
   
    * View the existing local git branch names
    ```sh
    git branch
    ```
    **Expected Output:**
    ```sh
    * main
    ```

    * Create a new git branch 
        NOTE: `-b <branch_name>` will need to be a unique (not one listed on GitHub!)
    
    ```sh
    git checkout -b working-jk
    ```
    
    **Expected Output:**
    ```sh
    Switched to a new branch 'working-jk'
    ```

    * Confirm you are _not_ on the "main" branch
    ```sh
    git branch
    ```

    **Expected Output:**
    ```sh
      main
    * working-jk
    ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>
