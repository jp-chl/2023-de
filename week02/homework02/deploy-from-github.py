from prefect import flow, task
from prefect.filesystems import GitHub

@task
def load_block_from_github():
  block = GitHub(
    repository="https://github.com/jp-chl/2023-de/"
  )

  block.get_directory("week02/homework02/")

  block.save("test")
  
  return block

@flow()
def main_flow() -> None:
  block = load_block_from_github()

if __name__ == "__main__":
  main_flow()