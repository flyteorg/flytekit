import weightwatcher as ww


class WeightWatcherRenderer:
    def to_html(self, model) -> str:
        watcher = ww.WeightWatcher(model=model)
        details = watcher.analyze()
        return df.to_html()
