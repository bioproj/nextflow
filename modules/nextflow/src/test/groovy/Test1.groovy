def view(Closure closure){
    def path = '/data/wangyang/nextflow/work/b7/64b38d3d335a522e01ffcbf4bb4142/x.txt'
    closure(path)
}

view{
    "Result： ${it}"
}
