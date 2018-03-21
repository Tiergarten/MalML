def get_samples(samples_dir):
    return [f for f in os.listdir(samples_dir) if re.match(r'^[A-Za-z0-9]{64}$', f, re.MULTILINE)]
