from django.conf import settings
from django.core.mail.backends.base import BaseEmailBackend

from djcelery_email.tasks import send_email

class CeleryEmailBackend(BaseEmailBackend):
    def __init__(self, fail_silently=False, **kwargs):
        super(CeleryEmailBackend, self).__init__(fail_silently)
        self.init_kwargs = kwargs
        self.batch_size  = getattr(settings, 'CELERY_EMAIL_BATCH_SIZE',
                                   kwargs.get('batch_size', 100))

    def send_messages(self, email_messages, **kwargs):
        results = []
        kwargs['_backend_init_kwargs'] = self.init_kwargs

        # slice up the messages into convenient chunks
        # even if the input happens to be an iterator
        batch = msgs = iter(email_messages)
        while msgs:
            batch = tuple(msgs.next() for i in xrange(0, self.batch_size))
            if batch:
                results.append(send_email.delay(batch, **kwargs))
            else:
                break
        return results
